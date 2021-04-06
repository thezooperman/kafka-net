using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace kafka
{
    public class KafkaConsumerHandler : BackgroundService
    {
        private readonly string topic = "simpletalk_topic";
        private readonly ILogger<KafkaConsumerHandler> logger;
        private readonly IConsumer<Null, string> _consumer;
        private readonly ConsumerConfig config = new ConsumerConfig
        {
            GroupId = "st_consumer_group",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        public KafkaConsumerHandler(ILogger<KafkaConsumerHandler> logger)
        {
            this.logger = logger;
            this._consumer = new ConsumerBuilder<Null, string>(config).Build();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Task.Factory.StartNew(async () => await StartLoop(stoppingToken), stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

            return Task.CompletedTask;
        }

        private Task StartLoop(CancellationToken cancellationToken)
        {
            logger.LogInformation("Staring the Hosted Service");


            this._consumer.Subscribe(topic);
            var cancelToken = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                logger.LogCritical("Cancellation requested, stopping the Hosted Service");
                cancelToken.CancelAfter(TimeSpan.FromSeconds(5));
            };

            try
            {
                while (true)
                {
                    var consumer = this._consumer.Consume(cancelToken.Token);
                    logger.LogInformation($"Message: {consumer.Message.Value} received from {consumer.TopicPartitionOffset}");
                    // await Task.Delay(1000);
                }
            }
            catch (ConsumeException cex)
            {
                logger.LogError(cex.InnerException?.Message ?? cex.Message);
                logger.LogCritical(cex.Error.Reason);
            }
            catch (OperationCanceledException oex)
            {
                logger.LogError(oex.InnerException?.Message ?? oex.Message);
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