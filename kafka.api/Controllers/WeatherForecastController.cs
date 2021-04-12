using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace kafka.api.Controllers
{
    [ApiController]
    [Route("api")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly CancellationTokenSource _cts;

        private KafkaDependentProducer<Null, string> _producer;
        private readonly string _topic;

        public WeatherForecastController(ILogger<WeatherForecastController> logger, IConfiguration config, KafkaDependentProducer<Null, string> producer)
        {
            _logger = logger;
            _cts = new CancellationTokenSource();
            _producer = producer;
            this._topic = config.GetValue<string>("Kafka:Topic");
        }

        [HttpGet]
        public IEnumerable<WeatherForecast> Get()
        {
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = rng.Next(-20, 55),
                Summary = Summaries[rng.Next(Summaries.Length)]
            })
            .ToArray();
        }

        [HttpPost("kafkaasync")]
        public async Task<IActionResult> Post([FromQuery] string message)
        {
            return Created(string.Empty, await Task.Run(() => SendToKafka(this._topic, message).ConfigureAwait(false)).ConfigureAwait(false));
        }
        private async Task SendToKafka(string topic, string message)
        {
            this._logger.LogInformation($"Send event for message - {message}");

            try
            {
                await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message }).ConfigureAwait(false);
            }
            catch (ProduceException<Null, string> pex)
            {
                _logger.LogError($"Oops, something went wrong: {pex.Error}");
                this._producer?.Flush(TimeSpan.FromSeconds(5));
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex.InnerException?.Message ?? ex.Message);
                this._producer?.Flush(TimeSpan.FromSeconds(5));
            }
        }

        [HttpPost("kafkasync")]
        public async Task<IActionResult> PostSync([FromQuery] string message)
        {
            Action<DeliveryReport<Null, string>> handler = r =>
            {
                if (r.Error.IsError || r.Status == PersistenceStatus.NotPersisted)
                    _logger.LogError($"Error: {r.Error.Reason}, Message - {r.Message.Value}");
                else
                    _logger.LogInformation($"Delivered message - {r.Topic}, with partition - {r.TopicPartition}, with offset - {r.Offset}");
            };

            try
            {
                this._producer.Produce(this._topic, new Message<Null, string> { Value = message }, handler);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                return BadRequest(ex.Message);
            }
            this._producer.Flush(TimeSpan.FromSeconds(5));
            return Ok();
        }
    }
}
