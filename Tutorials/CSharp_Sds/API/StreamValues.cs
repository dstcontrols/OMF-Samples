//Copyright 2019 OSIsoft, LLC
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

using System.Collections.Generic;

namespace IngressServiceAPI.API
{
    /// <summary>
    /// Holds a set of values for a stream.  The property names ContainerId and 
    /// Values are defined in the OMF spec.  This class is serialized into an 
    /// OMF message.
    /// </summary>
    public class StreamValues
    {
        public string ContainerId { get; set; }
        public IEnumerable<object> Values { get; set; }
    }
}