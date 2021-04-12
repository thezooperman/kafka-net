using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
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

        private readonly ProducerConfig _config;
        private IProducer<Null, string> _producer;

        public WeatherForecastController(ILogger<WeatherForecastController> logger)
        {
            _logger = logger;
            _cts = new CancellationTokenSource();
            _config = new ProducerConfig
            { BootstrapServers = "localhost:9092", Acks = Acks.Leader };
            _producer = new ProducerBuilder<Null, string>(_config).Build();
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

        private readonly string topic = "simpletalk_topic";
        [HttpPost("kafka")]
        public async Task<IActionResult> Post([FromQuery] string message)
        {
            // return Created(string.Empty, await SendToKafka(topic, message));

            return Created(string.Empty, await Task.Run(() => SendToKafka(topic, message).ConfigureAwait(false)).ConfigureAwait(false));
        }
        private async Task<Object> SendToKafka(string topic, string message)
        {
            this._logger.LogInformation($"Send event for message - {message}");

            try
            {
                return await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message }).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError($"Oops, something went wrong: {e}");
                this._producer?.Dispose();
            }
            return Task.CompletedTask;
        }
    }
}
