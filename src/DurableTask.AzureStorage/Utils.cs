//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.IO.Compression;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Newtonsoft.Json;

    [Flags]
    enum MessageFormatFlags
    {
        InlineJson = 0b0000,
        GZip = 0b0001,
    }

    static class Utils
    {
        public static readonly Task CompletedTask = Task.FromResult(0);

        static JsonSerializerSettings TaskMessageSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects
        };

        public static string SerializeMessageData(MessageData messageData)
        {
            string rawContent = JsonConvert.SerializeObject(messageData, TaskMessageSerializerSettings);
            return rawContent;
        }

        public static MessageData DeserializeQueueMessage(CloudQueueMessage queueMessage, string queueName)
        {
            ////string rawMessageData = queueMessage.AsString;

            ////// GZip message format: "gzip.{compressedBase64Payload}"
            ////const string GZipPrefix = "gzip";
            ////if (rawMessageData.StartsWith(GZipPrefix))
            ////{
            ////    string base64GZipPayload = rawMessageData.Substring(GZipPrefix.Length);
            ////    using (var memoryStream = new Memory)
            ////}

            MessageData envelope = JsonConvert.DeserializeObject<MessageData>(
                queueMessage.AsString,
                TaskMessageSerializerSettings);

            envelope.OriginalQueueMessage = queueMessage;
            envelope.TotalMessageSizeBytes = Encoding.UTF8.GetByteCount(queueMessage.AsString);
            envelope.QueueName = queueName;

            return envelope;
        }

        public static string Compress(SharedBufferManager bufferManager, string payload)
        {
            // Reference: https://blogs.msdn.microsoft.com/shacorn/2014/11/12/optimizing-memory-footprint-compression-in-net-4-5/
            int payloadByteCount = Encoding.Default.GetByteCount(payload);
            byte[] payloadBuffer = bufferManager.TakeBuffer(payloadByteCount);
            byte[] compressedBuffer = bufferManager.TakeBuffer(payloadByteCount);

            try
            {
                Encoding.Default.GetBytes(payload, 0, payload.Length, payloadBuffer, 0);

                using (var originStream = new MemoryStream(payloadBuffer, 0, payloadByteCount))
                using (var compressedStream = new MemoryStream(compressedBuffer, 0, payloadByteCount))
                {
                    using (var compressor = new GZipStream(compressedStream, CompressionLevel.Fastest))
                    {
                        compressor.Write(payloadBuffer, 0, payloadByteCount);
                    }


                    compressedStream.Length
                    lfdsjlkrsfjdsa
                        
                }
            }
            finally
            {
                bufferManager.ReturnBuffer(payloadBuffer);
                bufferManager.ReturnBuffer(compressedBuffer);
            }
        }

        internal static void CopyStreamWithoutAllocating(Stream origin, Stream destination, int bufferSize, SharedBufferManager bufferManager)
        {
            byte[] streamCopyBuffer = bufferManager.TakeBuffer(bufferSize);
            try
            {
                int read;
                while ((read = origin.Read(streamCopyBuffer, 0, bufferSize)) != 0)
                {
                    destination.Write(streamCopyBuffer, 0, read);
                }
            }
            finally
            {
                bufferManager.ReturnBuffer(streamCopyBuffer);
            }
        }

        public static async Task ParallelForEachAsync<TSource>(
            this IEnumerable<TSource> enumerable,
            Func<TSource, Task> createTask)
        {
            var tasks = new List<Task>();
            foreach (TSource entry in enumerable)
            {
                tasks.Add(createTask(entry));
            }

            await Task.WhenAll(tasks.ToArray());
        }
    }
}
