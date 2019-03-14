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

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace IngressServiceAPI.API
{
    /// <summary>
    /// DelegatingHandler to assist with authenticating with Identity Server.
    /// </summary>
    public class AuthenticationHandler : DelegatingHandler
    {
        #region Private Constants
        /// <summary>
        /// Authorization header name.
        /// </summary>
        private const string AuthorizationHeaderName = "Authorization";

        /// <summary>
        /// Bearer authentication scheme.
        /// </summary>
        private const string BearerAuthenticationScheme = "Bearer";

        /// <summary>
        /// Time (in seconds) to subtract from the Access Token expiry time to allow for successful HTTP requests to OCS resources 
        /// using a cached Access Token.
        /// </summary>
        private const int AccessTokenExpiryDelta = 30;

        /// <summary>
        /// Identity resource suffix.
        /// </summary>
        private const string IdentityResourceSuffix = "Identity/Connect/Token";

        #endregion

        #region Private Members

        /// <summary>
        /// Resource URL for OCS.
        /// </summary>
        private readonly Uri _serviceBaseUrl;

        /// <summary>
        /// Client Id.
        /// </summary>
        private readonly string _clientId;

        /// <summary>
        /// Client secret.
        /// </summary>
        private readonly string _clientSecret;

        /// <summary>
        /// Cached Access Token.
        /// </summary>
        private string _accessToken;

        /// <summary>
        /// Access Token expiry time.
        /// </summary>
        private DateTime _accessTokenExpiry = DateTime.MinValue;

        #endregion

        #region Public/Protected methods

        /// <summary>
        /// Create an instance of this DelegatingHandler.
        /// </summary>
        /// <param name="serviceBaseUrl">Base URL for OCS (e.g., "https://dat-b.osisoft.com")</param>
        /// <param name="clientId">Client Id of the Client to authenticate.</param>
        /// <param name="clientSecret">Client Secret of the Client to authenticate.</param>
        /// <exception cref="ArgumentNullException">One or more of the arguments are null.</exception>
        /// <exception cref="ArgumentException">One or more of the arguments only contain whitespace.</exception>
        public AuthenticationHandler(Uri serviceBaseUrl, string clientId, string clientSecret)
        {
            _serviceBaseUrl = serviceBaseUrl ?? throw new ArgumentNullException(nameof(serviceBaseUrl));

            if (clientId == null)
            {
                throw new ArgumentNullException(nameof(clientId));
            }

            if (clientId.Trim().Equals(string.Empty))
            {
                throw new ArgumentException("Parameter cannot be empty or whitespace", nameof(clientId));
            }

            _clientId = clientId;

            if (clientSecret == null)
            {
                throw new ArgumentNullException(nameof(clientSecret));
            }

            if (clientSecret.Trim().Equals(string.Empty))
            {
                throw new ArgumentException("Parameter cannot be empty or whitespace", nameof(clientSecret));
            }

            _clientSecret = clientSecret;

            InnerHandler = new HttpClientHandler();
        }

        /// <summary>
        /// Sends an HTTP request to the inner handler to send to the server as an asynchronous operation.
        /// </summary>
        /// <param name="request">The HTTP request message to send to the server.</param>
        /// <param name="cancellationToken">A cancellation token to cancel operation.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        /// <exception cref="System.ArgumentNullException">The request, resource, Client Id or Client Secret were null.</exception>
        /// <exception cref="AuthenticationHandlerException">Failure to authenticate and acquire an Access Token.</exception>
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // If there is an Authorization header in the HTTP request, remove it.
            request.Headers.Remove(AuthorizationHeaderName);

            // Get the Access Token and attach it to the Authorization header in the HTTP request.
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue(BearerAuthenticationScheme, await GetAccessToken(cancellationToken));

            // Make the HTTP request.
            return await base.SendAsync(request, cancellationToken);
        }

        #endregion

        #region Private methods

        /// <summary>
        /// Get the Access Token. If a valid cached Access Token is available, it will be returned - otherwise we authenticate and
        /// acquire an Access Token.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token to cancel operation.</param>
        /// <returns>Access Token.</returns>
        private async Task<string> GetAccessToken(CancellationToken cancellationToken)
        {
            // Check if we have a valid cached Access Token. If so, return it.
            if (_accessToken != null && !CachedAccessTokenExpired())
            {
                return _accessToken;
            }

            // Obtain a token from Identity Server via client credentials
            using (var client = new HttpClient())
            {
                var requestContent = new MultipartFormDataContent();
                var values = new Dictionary<string, string>
                {
                    { "grant_type", "client_credentials" },
                    { "client_id", _clientId },
                    { "client_secret", _clientSecret }
                };
                var content = new FormUrlEncodedContent(values);
                requestContent.Add(content);

                HttpResponseMessage response = await client.PostAsync(new Uri(_serviceBaseUrl, IdentityResourceSuffix), content);
                response.EnsureSuccessStatusCode();

                string json = await response.Content.ReadAsStringAsync();
                AccessToken tenantAdminToken = JsonConvert.DeserializeObject<AccessToken>(json);

                _accessToken = tenantAdminToken.TokenString;
                _accessTokenExpiry = DateTime.UtcNow.AddSeconds(tenantAdminToken.ExpiryTime - AccessTokenExpiryDelta);
                return _accessToken;
            }
        }

        /// <summary>
        /// Determines whether the cached Access Token has expired.
        /// </summary>
        /// <returns>Returns true if the Access Token has expired, false if it hasn't.</returns>
        private bool CachedAccessTokenExpired()
        {
            return (DateTime.UtcNow >= _accessTokenExpiry);
        }

        #endregion
    }
}
