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

using System;

namespace IngressServiceAPI
{
    /// <summary>
    /// Sample data structure to represent data objects in the target system.
    /// </summary>
    class SimpleType
    {
        public DateTime Time { get; set; }
        public double Value { get; set; }

        public const string JsonSchema =
            @"{
                ""id"": ""SimpleType"",
                ""type"": ""object"",
                ""classification"": ""dynamic"",
                ""properties"": {
                    ""Time"": { ""type"": ""string"", ""format"": ""date-time"", ""isindex"": true },
                    ""Value"": { ""type"": ""number"", ""format"": ""float64"" }
                }
            }";
    }
    
}
