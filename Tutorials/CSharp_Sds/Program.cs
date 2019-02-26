//Copyright 2018 OSIsoft, LLC
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//<http://www.apache.org/licenses/LICENSE-2.0>
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

using IngressServiceAPI.API;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;

namespace IngressServiceAPI
{
    class Program
    {
        static void Main(string[] args)
        {
            // Set this up in your app.config
            string ingressServiceUrl = ConfigurationManager.AppSettings["IngressServiceUrl"];
            string clientId = ConfigurationManager.AppSettings["ClientId"];
            string clientSecret = ConfigurationManager.AppSettings["ClientSecret"];

            IngressClient client = new IngressClient(ingressServiceUrl, clientId, clientSecret);

            // Use compression when sending data.  For such small sample messages, compression doesn't 
            // save us much space, but we're doing it here for demonstration sake.
            client.UseCompression = true;


            // 1) Send the Types message
            client.CreateTypes(new string[] { SimpleType.JsonSchema });
            client.CreateTypes(new string[] { ComplexType.JsonSchema });


            // 2) Send the Containers message
            ContainerInfo stream1 = new ContainerInfo() { Id = "TestStream1", TypeId = "SimpleType" };
            ContainerInfo stream2 = new ContainerInfo() { Id = "TestStream2", TypeId = "SimpleType" };
            ContainerInfo stream3 = new ContainerInfo() { Id = "TestStream3", TypeId = "ComplexType" };

            client.CreateContainers(new ContainerInfo[] { stream1, stream2, stream3 });


            // 3) Send the Values messages
            StreamValues complexValue = new StreamValues()
            {
                ContainerId = stream3.Id,
                Values = new List<ComplexType> { ComplexType.CreateSampleValue() }
            };

            client.SendValuesAsync(new StreamValues[] { complexValue }).Wait();

            // Here we loop indefinitely, sending 10 time series events to two streams every second.
            while (true)
            {
                // Create our set of values to send to our streams
                List<SimpleType> values = new List<SimpleType>();
                for(int i = 0; i < 10; i++)
                {
                    values.Add(new SimpleType() { Time = DateTime.UtcNow, Value = i });
                    Thread.Sleep(10);  // Offset the time-stamps by 10 ms
                }

                StreamValues vals1 = new StreamValues() { ContainerId = stream1.Id, Values = values };
                StreamValues vals2 = new StreamValues() { ContainerId = stream2.Id, Values = values };

                // Now send them
                client.SendValuesAsync(new StreamValues[] { vals1, vals2 }).Wait();

                Thread.Sleep(1000);
            }
        }
    }
}
