using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;

namespace consoleapp
{
    class Program
    {
        static Task Main(string[] args)
        {
            // const string topic_name = "sample_messages";

            #region Producer
            // var config = new ProducerConfig
            // {
            //     BootstrapServers = "localhost:9092",
            //     LingerMs = 5
            // };

            // // Create a producer that can be used to send messages to kafka that have no key and a value of type string 
            // using var p = new ProducerBuilder<Null, string>(config).Build();

            // var i = 501;
            // while (true)
            // {
            //     // Construct the message to send (generic type must match what was used above when creating the producer)
            //     var message = new Message<Null, string>
            //     {
            //         Value = $"Message #{++i}"
            //     };

            //     // Send the message to our test topic in Kafka                
            //     // var dr = await p.ProduceAsync("test", message);
            //     // Console.WriteLine($"Produced message '{dr.Value}' to topic {dr.Topic}, partition {dr.Partition}, offset {dr.Offset}");

            //     // var task = p.ProduceAsync("sample_messages", message);
            //     // task.ContinueWith(t =>
            //     // {
            //     //     if (t.IsFaulted)
            //     //         System.Console.WriteLine(t.Exception.Flatten().Message);
            //     //     else
            //     //         System.Console.WriteLine($"Produced message '{t.Result.Value}' to topic {t.Result.Topic}, partition {t.Result.Partition}, offset {t.Result.Offset}");
            //     // });

            //     Action<DeliveryReport<Null, string>> handler = r =>
            //     {
            //         var msg = $@"Deliverd message - {r.Topic}, with partition - {r.TopicPartition},
            //             topic partition - {r.TopicPartition},
            //             offset - {r.Offset}";
            //         System.Console.WriteLine(!r.Error.IsError ? msg : $"Error: {r.Error.Reason}");
            //     };
            //     p.Produce(topic_name, message, handler);

            //     if (i % 50 == 0)
            //         p.Flush(TimeSpan.FromSeconds(10));
            //     if (i > 700)
            //         break;
            //     // await Task.Delay(100);
            // }

            #endregion

            #region Consumer
            // var conf = new ConsumerConfig
            // {
            //     GroupId = "test-consumer-group",
            //     BootstrapServers = "localhost:9092",
            //     AutoOffsetReset = AutoOffsetReset.Earliest
            // };

            // using var c = new ConsumerBuilder<Ignore, string>(conf)
            // .Build();

            // c.Subscribe(topic_name);

            // // Because Consume is a blocking call, we want to capture Ctrl+C and use a cancellation token to get out of our while loop and close the consumer gracefully.
            // var cts = new CancellationTokenSource();
            // Console.CancelKeyPress += (_, e) =>
            // {
            //     e.Cancel = true;
            //     cts.Cancel();
            // };

            // try
            // {
            //     while (true)
            //     {
            //         // Consume a message from the test topic. Pass in a cancellation token so we can break out of our loop when Ctrl+C is pressed
            //         try
            //         {
            //             var cr = c.Consume(cts.Token);
            //             Console.WriteLine($"Consumed message '{cr.Message.Value}' from topic {cr.Topic}, partition {cr.Partition}, offset {cr.Offset}");
            //         }
            //         catch (ConsumeException cex)
            //         {
            //             System.Console.WriteLine(cex.InnerException?.Message ??
            //             $@"Record - {Encoding.UTF8.GetString(cex.ConsumerRecord.Message.Value)},
            //                Error - {cex.Error.Reason}");
            //         }
            //         // Do something interesting with the message you consumed
            //     }
            // }
            // catch (OperationCanceledException)
            // {
            // }
            // finally
            // {
            //     c.Close();
            // }

            #endregion

            #region Call-API
            // System.Net.ServicePointManager.ServerCertificateValidationCallback += (s, cert, chain, sslPolicyErrors) => true;
            using (var client = new HttpClient())
            {
                int messageLimit = 200;
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(
                                                new MediaTypeWithQualityHeaderValue("application/json"));

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    System.Diagnostics.Trace.TraceError("Cancellation requested", e);
                    e.Cancel = true;
                    cts.CancelAfter(TimeSpan.FromSeconds(5));
                };

                IList<Task<HttpResponseMessage>> tasks = new List<Task<HttpResponseMessage>>();
                while (messageLimit-- > 1)
                {
                    string message = $"Message #{messageLimit}";
                    message = System.Web.HttpUtility.UrlEncode(message);
                    try
                    {
                        var task = Task<HttpResponseMessage>.Run(async () =>
                        {
                            var uriBuilder = new UriBuilder($"http://localhost:5000/api/kafkasync?message={message}");
                            var msg = new HttpRequestMessage(HttpMethod.Post, uriBuilder.ToString());

                            return await client.SendAsync(msg, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
                        }, cts.Token);

                        task.ContinueWith((t) =>
                        {
                            if (t.IsCompletedSuccessfully)
                                System.Console.WriteLine($"Message - {t.Result.RequestMessage}, Status: {t.Result.StatusCode}");
                            else
                                System.Console.WriteLine(t.Exception.Flatten().Message);
                        }, cts.Token);

                        tasks.Add(task);
                    }
                    catch (InvalidOperationException iex)
                    {
                        System.Console.WriteLine(iex.InnerException?.Message ?? iex.Message);
                        break;
                    }
                    catch (HttpRequestException hex)
                    {
                        System.Console.WriteLine(hex.InnerException?.Message ?? hex.Message);
                    }
                    catch (TaskCanceledException tex)
                    {
                        System.Console.WriteLine(tex.InnerException?.Message ?? tex.Message);
                    }
                }
                try
                {
                    Task.WhenAll(tasks).Wait();
                }
                catch (System.AggregateException aex)
                {
                    System.Console.WriteLine(aex.Flatten().Message);
                }

                if (cts.IsCancellationRequested)
                    client.CancelPendingRequests();
            }

            #endregion

            return Task.CompletedTask;
        }
    }
}
