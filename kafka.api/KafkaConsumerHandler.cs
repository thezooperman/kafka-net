using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace kafka
{
    public class KafkaConsumerHandler : BackgroundService
    {
        private readonly string _topic;
        private readonly ILogger<KafkaConsumerHandler> _logger;
        private readonly IConsumer<Null, string> _consumer;
        private const int _commitPeriod = 5;

        public KafkaConsumerHandler(ILogger<KafkaConsumerHandler> logger, IConfiguration config)
        {
            this._logger = logger;
            var consumerConfig = new ConsumerConfig();
            config.GetSection("Kafka:ConsumerSettings").Bind(consumerConfig);
            this._topic = config.GetValue<string>("Kafka:Topic");
            this._consumer = new ConsumerBuilder<Null, string>(consumerConfig)
                                    .SetErrorHandler((_, e) => _logger.LogError($"Error:{e.Reason}"))
                                    .Build();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Task.Factory.StartNew(() => StartLoop(
                                        stoppingToken), stoppingToken,
                                        TaskCreationOptions.LongRunning,
                                        TaskScheduler.Default).ConfigureAwait(false);

            return Task.CompletedTask;
        }

        private Task StartLoop(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Staring the Hosted Service");


            this._consumer.Subscribe(_topic);
            var cancelToken = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                _logger.LogCritical("Cancellation requested, stopping the Hosted Service");
                cancelToken.CancelAfter(TimeSpan.FromSeconds(5));
            };


            while (true)
            {
                try
                {
                    var result = this._consumer.Consume(cancelToken.Token);
                    _logger.LogInformation($"Message: {result.Message.Value} received from {result.TopicPartitionOffset}");


                    if (result.IsPartitionEOF)
                        _logger.LogInformation($"Reached end of topic {result.Topic}, partition {result.Partition}, offset {result.Offset}.");

                    if (result.Offset % _commitPeriod == 0)
                    {
                        try
                        {
                            _consumer.Commit(result);
                        }
                        catch (KafkaException kex)
                        {
                            _logger.LogError(kex.Error.Reason);
                        }
                    }
                }
                catch (ConsumeException cex)
                {
                    _logger.LogCritical(cex.Error.Reason);

                    if (cex.Error.IsFatal)
                        break;
                }
                catch (OperationCanceledException oex)
                {
                    _logger.LogError(oex.InnerException?.Message ?? oex.Message);
                }
            }

            return Task.CompletedTask;
        }

        public override void Dispose()
        {
            this._consumer?.Close();
            this._consumer?.Dispose();
            base.Dispose();
        }

    }
}