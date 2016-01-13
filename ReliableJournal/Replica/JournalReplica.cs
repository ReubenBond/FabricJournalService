namespace ReliableJournal.Replica
{
    using System;
    using System.Diagnostics;
    using System.Fabric;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;

    using ProtoBuf.Meta;

    using ReliableJournal.Records;
    using ReliableJournal.Serialization;
    using ReliableJournal.Utilities;

    internal sealed class JournalReplica<TOperation> : IStatefulServiceReplica, IOperationApplier
    {
        private const string ReplicatorConfigSectionName = "ReplicatorConfig";

        private const string ConfigPackageName = "Config";

        private StatefulServiceInitializationParameters serviceParameters;
        
        private StateProvider stateProvider;

        private FabricReplicator fabricReplicator;

        private IStatefulServicePartition partition;

        private string logFilePath;

        private ReplicaRole unsafeRole;

        private ReplicaRole CurrentRole
        {
            get
            {
                ReplicaRole result;
                lock (this.replicaLock)
                {
                    result = this.unsafeRole;
                }
                
                return result;
            }

            set
            {
                lock (this.replicaLock)
                {
                    this.unsafeRole = value;
                }
            }
        }

        private OperationReceiver incomingOperations;
        
        private Logger logger;
        
        private readonly object replicaLock = new object();

        private OperationReplicator<TOperation> operationReplicator;

        private readonly IEventSourcedService<TOperation> service;

        private ICommunicationListener listener;

        private IReliableJournal<TOperation> reliableJournal;

        private readonly Serializer serializer;

        public JournalReplica(IEventSourcedService<TOperation> service, RuntimeTypeModel serializer = null)
        {
            if (service == null)
            {
                throw new ArgumentNullException(nameof(service));
            }

            this.serializer = CreateSerializer(serializer ?? TypeModel.Create());
            this.service = service;
        }

        private static Serializer CreateSerializer(RuntimeTypeModel model)
        {
            var recordMetadata = model.Add(typeof(OperationCommittedRecord), true);
            recordMetadata.AddSubType(10, typeof(OperationCommittedRecord<TOperation>));
            model.Add(typeof(EpochSurrogate), true);
            model.Add(typeof(Epoch), true).SetSurrogate(typeof(EpochSurrogate));

            Debug.WriteLine(
                model.GetSchema(typeof(OperationCommittedRecord<TOperation>)));

            return new Serializer(model);
        }

        public void Initialize(StatefulServiceInitializationParameters context)
        {
            this.serviceParameters = context;
            var replicaId = context.ReplicaId.ToString("X");
            var partitionKey = context.PartitionId.ToString("N");
            var workDirectory = context.CodePackageActivationContext.WorkDirectory;
            this.logFilePath = Path.Combine(workDirectory, $"journal_{partitionKey}_{replicaId}");
        }

        public async Task<IReplicator> OpenAsync(
            ReplicaOpenMode openMode,
            IStatefulServicePartition servicePartition,
            CancellationToken cancellationToken)
        {
            var self = this.serviceParameters;
            this.logger = new Logger(self) { Prefix = () => $"[{this.unsafeRole}] " };
            this.logger.Log("OpenAsync");
            IReplicator result;
            StateProvider provider;
            lock (this.replicaLock)
            {
                this.partition = servicePartition;
                provider = this.stateProvider = new StateProvider(this.logFilePath, this.logger, this.serializer);
                var replicatorSettings = ReplicatorSettings.LoadFrom(
                    self.CodePackageActivationContext,
                    ConfigPackageName,
                    ReplicatorConfigSectionName);
                replicatorSettings.BatchAcknowledgementInterval = TimeSpan.FromMilliseconds(1);
                result = this.fabricReplicator = servicePartition.CreateReplicator(this.stateProvider, replicatorSettings);
            }

            await provider.Initialize();
            this.logger.Log("Completed OpenAsync");
            return result;
        }

        public async Task<string> ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            var current = this.CurrentRole;
            var endpointAddress = default(string);

            this.logger.Log($"Transitioning from {current} to {newRole}.");
            switch (current)
            {
                case ReplicaRole.Unknown:
                    switch (newRole)
                    {
                        case ReplicaRole.None:
                            await this.CompleteReceivingOperations();
                            await this.service.Reset(cancellationToken);
#warning Delete local state.
                            break;
                        case ReplicaRole.IdleSecondary:
                        case ReplicaRole.ActiveSecondary:
                            this.StartReceivingOperations();
                            break;
                        case ReplicaRole.Primary:
                            endpointAddress = await this.StartReplicatingOutgoingOperations(cancellationToken);
                            break;
                    }
                    break;
                case ReplicaRole.Primary:
                    switch (newRole)
                    {
                        case ReplicaRole.None:
                            // Wait for all pending requests to complete.
                            await this.StopReplicatingOutgoingOperations(cancellationToken);
                            await this.service.Reset(cancellationToken);
#warning Delete local state.
                            break;
                        case ReplicaRole.ActiveSecondary:
                            // Wait for all pending requests to complete.
                            await this.StopReplicatingOutgoingOperations(cancellationToken);

                            // Create an operation pump and start pumping operations.
                            this.StartReceivingOperations();
                            break;
                    }
                    break;
                case ReplicaRole.IdleSecondary:
                    switch (newRole)
                    {
                        case ReplicaRole.None:
                            await this.CompleteReceivingOperations();
                            await this.service.Reset(cancellationToken);
#warning Delete local state.
                            break;
                        case ReplicaRole.ActiveSecondary:
                            // Wait for copying to complete and for replication to have begun.
                            await this.incomingOperations.ReplicationInitiated;
                            break;
                        case ReplicaRole.Primary:
                            await this.CompleteReceivingOperations();

                            // Create a replicator so that operations can be replicated from this instance.
                            endpointAddress = await this.StartReplicatingOutgoingOperations(cancellationToken);
                            break;
                    }
                    break;
                case ReplicaRole.ActiveSecondary:
                    switch (newRole)
                    {
                        case ReplicaRole.None:
                            await this.CompleteReceivingOperations();
                            await this.service.Reset(cancellationToken);
#warning Delete local state.
                            break;
                        case ReplicaRole.Primary:
                            await this.CompleteReceivingOperations();

                            endpointAddress = await this.StartReplicatingOutgoingOperations(cancellationToken);
                            break;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException($"Transition from {current} to {newRole} is invalid.");
            }

            this.CurrentRole = newRole;

            var listenMessage = !string.IsNullOrWhiteSpace(endpointAddress)
                                    ? $"Listening on '{endpointAddress}'"
                                    : string.Empty;
            this.logger.Log($"Transitioned from {current} to {newRole}.{listenMessage}");
            return endpointAddress;
        }

        /// <summary>
        /// Starts replicating outgoing operations.
        /// </summary>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <remarks>
        /// This should be called on transition into the <see cref="ReplicaRole.Primary"/> role.
        /// </remarks>
        private async Task<string> StartReplicatingOutgoingOperations(CancellationToken cancellationToken)
        {
            this.logger.Log(nameof(this.StartReplicatingOutgoingOperations));
            this.operationReplicator = new OperationReplicator<TOperation>(
                this.fabricReplicator.StateReplicator2,
                this.stateProvider,
                this.logger,
                this.serializer);

            // Open the service.
            this.reliableJournal = new ReliableJournal<TOperation>(this.operationReplicator, this.service);
            this.listener = this.service.CreateCommunicationListener(this.serviceParameters);
            await this.service.Open(this.reliableJournal, cancellationToken);

            // Apply existing events
            foreach (var entry in this.stateProvider.GetOperations())
            {
                var operationRecord = this.serializer.Deserialize<Record>(entry.Data) as OperationCommittedRecord<TOperation>;
                if (operationRecord == null)
                {
                    continue;
                }

                var eventOperation = operationRecord.Operation;
                if (eventOperation != null)
                {
                    //this.logger.Log($"Applying stored event {eventOperation.Event}");
                    await this.service.Apply(eventOperation, cancellationToken);
                }
            }

            this.logger.Log("Completed " + nameof(this.StartReplicatingOutgoingOperations));
            return await this.listener.OpenAsync(cancellationToken);
        }

        /// <summary>
        /// Stops replicating outgoing operations.
        /// </summary>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <remarks>
        /// This should be called on transition out of the <see cref="ReplicaRole.Primary"/> role.
        /// </remarks>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private async Task StopReplicatingOutgoingOperations(CancellationToken cancellationToken)
        {
            this.logger.Log(nameof(this.StopReplicatingOutgoingOperations));
            var serviceListener = Interlocked.Exchange(ref this.listener, null);
            if (serviceListener != null)
            {
                await serviceListener.CloseAsync(cancellationToken);
            }

            if (this.service != null)
            {
                await this.service.Close(cancellationToken);
            }

            this.reliableJournal = null;

            var pusher = Interlocked.Exchange(ref this.operationReplicator, null);
            if (pusher != null)
            {
                await pusher.Close();
            }

            this.logger.Log("Completed " + nameof(this.StopReplicatingOutgoingOperations));
        }

        /// <summary>
        /// Starts receiving copy and replication operations from other replicas.
        /// </summary>
        /// <remarks>
        /// This should be called on transition into the <see cref="ReplicaRole.IdleSecondary"/> and <see cref="ReplicaRole.ActiveSecondary"/> roles.
        /// </remarks>
        private void StartReceivingOperations()
        {
            this.logger.Log(nameof(this.StartReceivingOperations));
            this.incomingOperations = new OperationReceiver(this, this.fabricReplicator.StateReplicator2, this.logger);
            this.incomingOperations.Start(CancellationToken.None);
            this.logger.Log("Completed " + nameof(this.StartReceivingOperations));
        }

        /// <summary>
        /// Stops replicating outgoing operations.
        /// </summary>
        /// <remarks>
        /// This should be called on transition out of the <see cref="ReplicaRole.IdleSecondary"/> and <see cref="ReplicaRole.ActiveSecondary"/> roles.
        /// </remarks>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        private async Task CompleteReceivingOperations()
        {
            this.logger.Log(nameof(this.CompleteReceivingOperations));
            var pump = Interlocked.Exchange(ref this.incomingOperations, null);
            if (pump != null)
            {
                await pump.Completed;
            }

            this.logger.Log("Completed " + nameof(this.CompleteReceivingOperations));
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            this.logger.Log(nameof(this.CloseAsync));
            this.stateProvider?.Dispose();
            this.stateProvider = null;

            this.logger.Log("Completed " + nameof(this.CloseAsync));
            return Task.FromResult(0);
        }

        public void Abort()
        {
            this.logger.Log(nameof(this.Abort));
            this.stateProvider?.Dispose();
            this.stateProvider = null;
            this.logger.Log("Completed " + nameof(this.Abort));
        }

        /// <summary>
        /// Applies the provided, copied <paramref name="operation"/>.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="cancellationtoken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public async Task ApplyCopyOperation(IOperation operation, CancellationToken cancellationtoken)
        {
            try
            {
                foreach (var operationData in operation.Data)
                {
                    var record = this.serializer.Deserialize<Record>(operationData);

                    var operationCommittedRecord = record as OperationCommittedRecord;
                    if (operationCommittedRecord != null)
                    {
                        // Append the operation to the log at the specified epoch.
                        await
                            this.stateProvider.AppendOperation(
                                operationData,
                                operationCommittedRecord.Version.LogSequenceNumber);
                    }
                }
            }
            catch (Exception exception)
            {
                this.logger.Log($"Exception in {nameof(this.ApplyCopyOperation)}: {exception}");
                this.partition.ReportFault(FaultType.Transient);
                throw;
            }
        }

        /// <summary>
        /// Applies the provided, replicated <paramref name="operation"/>.
        /// </summary>
        /// <param name="operation">The operation.</param>
        /// <param name="cancellationtoken">The cancellation token.</param>
        /// <returns>A <see cref="Task"/> representing the work performed.</returns>
        public async Task ApplyReplicationOperation(IOperation operation, CancellationToken cancellationtoken)
        {
            try
            {
                foreach (var operationData in operation.Data)
                {
                    var op = this.serializer.Deserialize<TOperation>(operationData);

                    // Create a record to append.
                    var version = new RecordVersion(this.stateProvider.CurrentEpoch, operation.SequenceNumber);
                    var record = new OperationCommittedRecord<TOperation>(op, version);

                    // Append the operation to the log at the current epoch.
                    await
                        this.stateProvider.AppendOperation(this.serializer.Serialize<Record>(record), operation.SequenceNumber);
                }
            }
            catch (Exception exception)
            {
                this.logger.Log($"Exception in {nameof(this.ApplyReplicationOperation)}: {exception}");
                this.partition.ReportFault(FaultType.Transient);
                throw;
            }
        }
    }
}