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
    class ComplexType
    {
        public DateTime DateTimeProperty { get; set; }
        public string StringProperty { get; set; }
        public double DoubleProperty { get; set; }
        public int IntegerProperty { get; set; }
        public ComplexType_Nested1 ObjectProperty { get; set; }

        public static ComplexType CreateSampleValue()
        {
            ComplexType sample = new ComplexType();
            sample.DateTimeProperty = DateTime.UtcNow;
            sample.StringProperty = "SampleValue";
            sample.DoubleProperty = 1.1;
            sample.IntegerProperty = 2;
            sample.ObjectProperty = new ComplexType_Nested1()
            {
                NumberProperty = 3.3,
                IntegerArrayProperty = new int[] { 1, 2, 3 },
                NestedObjectProperty = new ComplexType_Nested2()
                {
                    NumberProperty = 4.4,
                    StringProperty = "Nested string value"
                }
            };

            return sample;
        }

        public static string JsonSchema =
        @"{
            ""id"": ""ComplexType"",
            ""description"": ""A complex type including strings, ints, doubles, arrays, and nested objects"",
            ""type"": ""object"",
            ""classification"": ""dynamic"",
            ""properties"": {
                ""DateTimeProperty"": { ""type"": ""string"", ""format"": ""date-time"", ""isindex"": true },
                ""StringProperty"": { ""type"": ""string""},
                ""DoubleProperty"": { ""type"": ""number"", ""format"": ""float64"" },
                ""IntegerProperty"": { ""type"": ""integer"" },
                ""ObjectProperty"": {
                    ""type"": ""object"",
                    ""properties"": {
                        ""NumberProperty"": { ""type"": ""number"", ""format"": ""float64"" },
                        ""IntegerArrayProperty"": { ""type"": ""array"", ""items"": { ""type"": ""integer"" }, ""maxItems"": 3 },
                        ""NestedObjectProperty"": {
                            ""type"": ""object"",
                            ""properties"": {
                                ""NumberProperty"": { ""type"": ""number"", ""format"": ""float64"" },
                                ""StringProperty"": { ""type"": ""string"" }
                            }
                        }
                    }
                }
            }
        }";  
    }

    class ComplexType_Nested1
    {
        public double NumberProperty { get; set; }
        public int[] IntegerArrayProperty { get; set; }
        public ComplexType_Nested2 NestedObjectProperty { get; set; }
    }

    class ComplexType_Nested2
    {
        public double NumberProperty { get; set; }
        public string StringProperty { get; set; }
    }
}
