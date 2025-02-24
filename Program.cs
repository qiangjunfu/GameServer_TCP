//// See https://aka.ms/new-console-template for more information

using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;


public class GameServer
{
    private static readonly ConcurrentDictionary<string, Socket> Clients = new();
    private static readonly ConcurrentDictionary<Socket, int> ClientIds = new();
    private static TcpListener serverListener;
    private static int clientIdCounter = 1;
    private const int Port = 12345;
    private const int BufferSize = 1024;
    private const int maxClientCount = 50;
    private static readonly string LogFilePath = "server_log.txt"; // 日志文件路径


    private static readonly ConcurrentQueue<MessageWithSender> MessageQueue = new();
    private static readonly int FrameRate = 60;




    public static async Task Main(string[] args)
    {
        Console.CancelKeyPress += async (sender, e) =>
        {
            e.Cancel = true; // 防止程序直接退出
            await ShutdownServer();
        };
        //await StartServer();
        Task startServerTask = StartServer();

        int frameInterval = (int)(1000 / FrameRate);
        while (true)
        {
            await Task.Delay(frameInterval);

            await ProcessQueuedMessages();
        }
    }
    public static async Task ProcessQueuedMessages()
    {
        while (true)
        {
            if (!MessageQueue.IsEmpty)
            {
                var tasks = new List<Task>();
                while (MessageQueue.TryDequeue(out MessageWithSender? messageWithSender))
                {
                    tasks.Add(BroadcastNetworkMessage(messageWithSender.SenderSocket, messageWithSender.Message));
                }
                await Task.WhenAll(tasks);

                Log($"MessageQueue  tasksCount : {tasks.Count} ");
            }

            await Task.Delay(10);
        }
    }



    private static readonly ConcurrentDictionary<string, Room> Rooms = new(); // 存储所有房间
    private static readonly ConcurrentDictionary<Socket, string> ClientRooms = new(); // 存储每个客户端所在的房间

    private static readonly ConcurrentDictionary<int, ClientSession> ClientSessions = new();  // 用于存储客户端会话信息

    public static async Task StartServer()
    {
        try
        {
            serverListener = new TcpListener(IPAddress.Any, Port);
            serverListener.Start();

            string localIP = GetLocalIPAddress();
            Log($"服务器已启动，本机IP：{localIP}，监听端口 {Port}，等待客户端连接...");

            while (true)
            {
                TcpClient tcpClient = await serverListener.AcceptTcpClientAsync();
                Socket clientSocket = tcpClient.Client;

                if (Clients.Count >= maxClientCount)
                {
                    Log($"连接已达到最大限制，拒绝客户端: {clientSocket.RemoteEndPoint}");
                    byte[] rejectMessage = Encoding.UTF8.GetBytes("Server is full. Connection rejected.");
                    await clientSocket.SendAsync(new ArraySegment<byte>(rejectMessage), SocketFlags.None);
                    clientSocket.Close();
                    continue; // 不接受这个客户端，直接跳过
                }

                string clientKey = $"{clientSocket.RemoteEndPoint}___{clientIdCounter}";
                Clients[clientKey] = clientSocket;
                int clientId = clientIdCounter;
                ClientIds[clientSocket] = clientId;
                clientIdCounter += 1;
                Log($"客户端连接: {clientSocket.RemoteEndPoint}，客户端ID: {clientId}");

                byte[] idMessage = Encoding.UTF8.GetBytes(clientId.ToString());
                await clientSocket.SendAsync(new ArraySegment<byte>(idMessage), SocketFlags.None);
                Log($"已向 {clientSocket.RemoteEndPoint} 发送客户端ID: {clientId}");


                await JoinRoom(clientSocket, defaultRoomId);
                #region 断线重连
                //逻辑应该是: 服务器向客户端发送一个已连接消息,客户端通过消息先判断本地有没有clientId,
                //            如果没有向服务器请求一个新clientId , 如果有就直接发送本地保存的clientId
                //string msg = $"客户端: {clientSocket.RemoteEndPoint}  连接到服务器: {GetLocalIPAddress()}";
                //byte[] message = Encoding.UTF8.GetBytes(msg);
                //await clientSocket.SendAsync(new ArraySegment<byte>(message), SocketFlags.None);
                //Log(msg);

                #endregion


                _ = Task.Run(() => HandleClient(clientSocket));
            }
        }
        catch (Exception ex)
        {
            Log($"服务器启动失败: {ex.Message}");
        }
    }


    private static readonly ConcurrentDictionary<Socket, DateTime> clientHeartbeats = new();  // 用来存储每个客户端的心跳时间戳
    public static async Task HandleClient(Socket clientSocket)
    {
        byte[] buffer = new byte[BufferSize];
        CircularBuffer dataBuffer = new CircularBuffer(BufferSize * 100); // 创建一个环形缓冲区

        var lastHeartbeat = DateTime.Now;  // 最后一次收到心跳包的时间
        var heartbeatInterval = TimeSpan.FromSeconds(9);  // 心跳包时间间隔


        try
        {
            while (true)
            {
                if ((DateTime.Now - clientHeartbeats.GetOrAdd(clientSocket, _ => DateTime.Now)) > heartbeatInterval)
                {
                    Log($"心跳超时，{GetClientIdPoint(clientSocket)} 断开连接 ... ");
                    await LeaveRoom(clientSocket);  // 离开房间
                    RemoveClient(clientSocket);
                    break;
                }


                // 接收数据
                int bytesReceived = await clientSocket.ReceiveAsync(new ArraySegment<byte>(buffer), SocketFlags.None);
                if (bytesReceived == 0)
                {
                    var clientEndPoint = clientSocket.RemoteEndPoint;
                    var clientId = ClientIds[clientSocket];
                    Log($"客户端 {GetClientIdPoint(clientSocket)} 主动断开连接");
                    await LeaveRoom(clientSocket);  // 离开房间
                    RemoveClient(clientSocket);
                    break;
                }


                // 将接收到的数据写入环形缓冲区
                dataBuffer.Write(buffer, 0, bytesReceived);

                // 尝试解析完整的数据包
                while (dataBuffer.Length >= 10) // 至少包含包头(6字节) + 数据长度(4字节)
                {
                    dataBuffer.Peek(0, 6, out byte[] header);
                    string headerStr = Encoding.UTF8.GetString(header);
                    if (headerStr != "HEADER")
                    {
                        Log("无效包头，丢弃数据");
                        dataBuffer.Discard(6);
                        continue;
                    }

                    // 读取数据长度
                    dataBuffer.Peek(6, 4, out byte[] lengthBytes);
                    int bodyLength = BitConverter.ToInt32(lengthBytes, 0);

                    if (dataBuffer.Length < 10 + bodyLength)
                    {
                        Log("数据包不完整，等待更多数据...");
                        break;
                    }

                    // 读取完整数据包
                    dataBuffer.Read(10 + bodyLength, out byte[] fullPacket);
                    byte[] body = fullPacket[10..]; // 提取消息体



                    // 如果消息是心跳包，更新最后一次收到心跳包的时间
                    if (Encoding.UTF8.GetString(body).Equals("HEARTBEAT"))
                    {
                        clientHeartbeats[clientSocket] = DateTime.Now;  // 更新客户端的最后心跳时间
                        Log($"{GetClientIdPoint(clientSocket)} 收到心跳包，保持连接 ...");
                    }
                    else
                    {
                        if (TryParseNetworkMessage(body, out NetworkMessage? networkMessage))
                        {
                            if (networkMessage == null) return;

                            switch (networkMessage?.MessageType)
                            {
                                case NetworkMessageType.GetClientId:
                                    ClientIdMessage clientIdMessage = ByteArrayToJson<ClientIdMessage>(networkMessage.Data);
                                    if (clientIdMessage != null)
                                    {
                                        await ProcessMessage_GetClientId(clientSocket, clientIdMessage);
                                    }
                                    break;
                                case NetworkMessageType.JoinRoom:
                                    break;
                                case NetworkMessageType.LeaveRoom:
                                    //RoomMessage roomMessage2 = HandleRoomMessage(networkMessage.Data);
                                    //await SwitchRoom(clientSocket, roomMessage2.roomId);
                                    break;
                                case NetworkMessageType.SwitchRoom:
                                    RoomMessage roomMessage = HandleRoomMessage(networkMessage.Data);
                                    if (roomMessage != null && roomMessage.roomId != null)
                                    {
                                        await SwitchRoom(clientSocket, roomMessage.roomId);
                                    }
                                    break;
                                default:
                                    Log($"收到来自客户端 {GetClientIdPoint(clientSocket)} NetworkMessage: 类型={networkMessage.MessageType}");
                                    // 在接收消息时，把发送者和消息一起入队
                                    MessageQueue.Enqueue(new MessageWithSender(clientSocket, networkMessage));
                                    break;
                            }
                        }
                        //Log($"收到来自客户端 {GetClientIdPoint(clientSocket)} NetworkMessage: 类型={networkMessage.MessageType}");
                        //MessageQueue.Enqueue(networkMessage);
                        ////await ProcessMessage(clientSocket, body);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Log($"与客户端 {clientSocket.RemoteEndPoint} 通信时发生错误: {ex.Message}");
        }
        finally
        {
            RemoveClient(clientSocket);
        }
    }


    #region 接收消息处理
    public static async Task ProcessMessage(Socket clientSocket, byte[] message)
    {
        if (TryParseNetworkMessage(message, out NetworkMessage networkMessage))
        {
            Log($"收到来自客户端 {GetClientIdPoint(clientSocket)} NetworkMessage: 类型={networkMessage.MessageType}");
            await BroadcastNetworkMessage(clientSocket, networkMessage);
        }
        else
        {
            string receivedMessage = Encoding.UTF8.GetString(message);
            Log($"收到来自客户端 {GetClientIdPoint(clientSocket)} 的消息: {receivedMessage}");
            await BroadcastMessage(clientSocket, receivedMessage);
        }
    }

    public static async Task ProcessMessage_GetClientId(Socket clientSocket, ClientIdMessage clientIdMessage)
    {
        //如果客户端没有ID 则服务器分配一个 然后发送给客户端
        // 如果有ID 则检查是否在ClientSessions中,如果在则是断线重连,如果不在则是新客户端
        if (clientIdMessage.ClientId == -1)
        {
            string clientKey = $"{clientSocket.RemoteEndPoint}___{clientIdCounter}";
            Clients[clientKey] = clientSocket;
            int clientId = clientIdCounter;
            ClientIds[clientSocket] = clientId;
            clientIdCounter += 1;
            Log($"客户端连接: {clientSocket.RemoteEndPoint}， 客户端分配的新ID: {clientId}");

            ClientIdMessage clientIdMessage2 = new ClientIdMessage
            {
                ClientId = ClientIds[clientSocket],
                ClientType = clientIdMessage.ClientType,
                GlobalObjId = clientIdMessage.GlobalObjId
            };
            NetworkMessage networkMessage = new NetworkMessage(NetworkMessageType.GetClientId, JsonToByteArray<ClientIdMessage>(clientIdMessage2));
            byte[] combinedMessage = PrepareNetworkMessage(networkMessage);
            await clientSocket.SendAsync(new ArraySegment<byte>(combinedMessage), SocketFlags.None);
        }
        else
        {
            // 新客户端，加入默认房间并保存会话
            if (!ClientSessions.ContainsKey(clientIdMessage.ClientId))
            {
                await JoinRoom(clientSocket, defaultRoomId);
                ClientSessions[clientIdMessage.ClientId] = new ClientSession
                {
                    ClientId = clientIdMessage.ClientId,
                    RoomId = defaultRoomId,
                    LastActiveTime = DateTime.Now,
                    WasNormalExit = false // 设置为false，表示这是正常进入
                };
            }
            else   // 旧客户端
            {
                // 如果是断线重连，检查是否正常退出
                var session = ClientSessions[clientIdMessage.ClientId];
                if (session.WasNormalExit)
                {
                    // 正常退出，进入默认大厅
                    await JoinRoom(clientSocket, defaultRoomId);
                    Log($"客户端 {clientIdMessage.ClientId} 正常退出，重新进入默认大厅.");
                }
                else
                {
                    // 断线重连，恢复之前的房间
                    await JoinRoom(clientSocket, session.RoomId);
                    Log($"客户端 {clientIdMessage.ClientId} 断线重连，恢复房间 {session.RoomId}.");
                }
            }
        }
    }


    #endregion


    #region 广播消息处理

    public static async Task BroadcastMessage(Socket senderSocket, string message)
    {
        string senderRoomId;
        if (senderSocket != null && ClientRooms.ContainsKey(senderSocket))
        {
            senderRoomId = ClientRooms[senderSocket];  // 如果包含，直接取值
        }
        else
        {
            if (senderSocket != null && senderSocket.Connected)
                Log($"客户端 {senderSocket?.RemoteEndPoint} 不在房间列表中");
            else
                Log($"客户端 senderSocket == null ");
            return;
        }


        byte[] combinedMessage = PrepareMessage(message);

        Log(" ------------------------广播开始------------------------");
        foreach (var kvp in Clients)
        {
            var clientKey = kvp.Key;
            var clientSocket = kvp.Value;

            try
            {
                if (clientSocket.Connected && ClientRooms.ContainsKey(clientSocket) && ClientRooms[clientSocket] == senderRoomId)
                {
                    await clientSocket.SendAsync(new ArraySegment<byte>(combinedMessage), SocketFlags.None);
                    Log($"广播消息给客户端: {clientKey}");
                }
                else
                {
                    Log($"检测到断开的客户端: {clientKey}");
                    Clients.TryRemove(clientKey, out _);
                    ClientIds.TryRemove(clientSocket, out _);
                }
            }
            catch (Exception ex)
            {
                Log($"广播消息失败: {ex.Message}");
                Clients.TryRemove(clientKey, out _);
                ClientIds.TryRemove(clientSocket, out _);
            }
        }
        Log(" ------------------------广播结束------------------------");
    }
    public static async Task BroadcastNetworkMessage(Socket senderSocket, NetworkMessage networkMessage)
    {
        string senderRoomId;
        if (senderSocket != null && ClientRooms.ContainsKey(senderSocket))
        {
            senderRoomId = ClientRooms[senderSocket];  // 如果包含，直接取值
        }
        else
        {
            if (senderSocket != null && senderSocket.Connected)
                Log($"客户端 {senderSocket?.RemoteEndPoint} 不在房间列表中");
            else
                Log($"客户端 senderSocket == null ");
            return;
        }

        byte[] combinedMessage = PrepareNetworkMessage(networkMessage);
        Log($" ------------------------广播网络数据开始: {networkMessage.MessageType.ToString()}------------------------");
        var tasks = new List<Task>();
        foreach (var kvp in Clients)
        {
            var clientKey = kvp.Key;
            var clientSocket = kvp.Value;

            if (clientSocket.Connected && ClientRooms.ContainsKey(clientSocket) && ClientRooms[clientSocket] == senderRoomId)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await clientSocket.SendAsync(new ArraySegment<byte>(combinedMessage), SocketFlags.None);
                        Log($"广播网络数据给客户端: {clientKey}");
                    }
                    catch (Exception ex)
                    {
                        Log($"广播网络数据到客户端 {kvp.Key} 失败: {ex.Message}");
                        RemoveClient(clientSocket);
                    }
                }));
            }
        }

        await Task.WhenAll(tasks);
        Log(" ------------------------广播网络数据结束------------------------");
    }
    public static async Task BroadcastClientJoinOrLeave(Socket clientSocket, string roomId, bool isJoin)
    {
        NetworkMessageType _type = isJoin ? NetworkMessageType.JoinRoom : NetworkMessageType.LeaveRoom;
        RoomMessage roomMessage = new RoomMessage
        {
            ClientId = ClientIds[clientSocket],
            ClientType = 1,
            GlobalObjId = 1,

            roomMessageType = _type.ToString(),
            roomId = roomId
        };
        //print("客户端发送的数据内容: " + roomMessage.PrintInfo());
        NetworkMessage networkMessage = new NetworkMessage(_type, JsonToByteArray<RoomMessage>(roomMessage));
        byte[] combinedMessage = PrepareNetworkMessage(networkMessage);
        Log($" ------------------------广播房间消息: {GetClientIdPoint(clientSocket)}  roomMessageType:{roomMessage.roomMessageType}  {roomId}------------------------");
        Room room = Rooms[roomId];
        List<Task> tasks = new List<Task>();
        foreach (var client in room.Clients)
        {
            //Log($"{roomId} 房间中的客户端: {GetClientIdPoint(client)}");
            //if (client != clientSocket)   // 不发送给自己
            //{
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    await client.SendAsync(new ArraySegment<byte>(combinedMessage), SocketFlags.None);
                    Log($"{roomId} 广播房间消息到客户端: {GetClientIdPoint(client)}");
                }
                catch (Exception ex)
                {
                    Log($"{roomId} 广播房间消息到客户端 {GetClientIdPoint(client)} 失败: {ex.Message}");
                    RemoveClient(client);
                }
            }));
            //}
        }
        await Task.WhenAll(tasks);
        Log(" ------------------------广播房间消息结束------------------------");
    }


    public static byte[] PrepareMessage(string message)
    {
        byte[] header = Encoding.UTF8.GetBytes("HEADER");
        byte[] messageBytes = Encoding.UTF8.GetBytes(message);
        byte[] lengthBytes = BitConverter.GetBytes(messageBytes.Length);

        byte[] combinedMessage = new byte[header.Length + lengthBytes.Length + messageBytes.Length];
        Array.Copy(header, 0, combinedMessage, 0, header.Length);
        Array.Copy(lengthBytes, 0, combinedMessage, header.Length, lengthBytes.Length);
        Array.Copy(messageBytes, 0, combinedMessage, header.Length + lengthBytes.Length, messageBytes.Length);

        return combinedMessage;
    }
    public static byte[] PrepareNetworkMessage(NetworkMessage networkMessage)
    {
        byte[] header = Encoding.UTF8.GetBytes("HEADER");
        byte[] typeBytes = BitConverter.GetBytes((int)networkMessage.MessageType);
        byte[] dataBytes = networkMessage.Data;
        byte[] lengthBytes = BitConverter.GetBytes(typeBytes.Length + dataBytes.Length);

        byte[] combinedMessage = new byte[header.Length + lengthBytes.Length + typeBytes.Length + dataBytes.Length];
        Array.Copy(header, 0, combinedMessage, 0, header.Length);
        Array.Copy(lengthBytes, 0, combinedMessage, header.Length, lengthBytes.Length);
        Array.Copy(typeBytes, 0, combinedMessage, header.Length + lengthBytes.Length, typeBytes.Length);
        Array.Copy(dataBytes, 0, combinedMessage, header.Length + lengthBytes.Length + typeBytes.Length, dataBytes.Length);

        //Log($"PrepareNetworkMessage() - 包头长度: {header.Length}, 包体长度: {lengthBytes.Length}, 消息类型长度: {typeBytes.Length}, 数据长度: {dataBytes.Length}, 总长度: {combinedMessage.Length}");

        return combinedMessage;
    }

    #endregion


    #region 房间管理
    static string defaultRoomId = "defaultRoom";

    public static async Task JoinRoom(Socket clientSocket, string roomId)
    {
        if (!Rooms.ContainsKey(roomId))
        {
            Rooms[roomId] = new Room(roomId);
            Log($"房间 {roomId} 不存在 , 创建房间 .");
        }

        Rooms[roomId].AddClient(clientSocket);
        ClientRooms[clientSocket] = roomId;  // 记录客户端所属的房间
        Log($"客户端 {GetClientIdPoint(clientSocket)} 已加入房间 {roomId}");

        await BroadcastClientJoinOrLeave(clientSocket, roomId, true);
    }

    public static async Task<bool> LeaveRoom(Socket clientSocket)
    {
        if (!ClientRooms.ContainsKey(clientSocket))
        {
            Log($"客户端 {GetClientIdPoint(clientSocket)}  没有加入任何房间.");
            return false;
        }

        string currentRoomId = ClientRooms[clientSocket];  // 记录当前房间
        Room currentRoom = Rooms[currentRoomId];

        currentRoom.RemoveClient(clientSocket);  // 从房间中移除客户端
        ClientRooms.TryRemove(clientSocket, out _);  // 移除客户端的房间记录
        await BroadcastClientJoinOrLeave(clientSocket, currentRoomId, false); // 异步广播客户端离开房间

        //await JoinRoom(clientSocket, defaultRoomId);        // 将客户端加入默认大厅房间
        //Log($"客户端 {GetClientIdPoint(clientSocket)} 已离开房间 {currentRoomId} , 并重新加入大厅房间 {defaultRoomId}");
        Log($"客户端 {GetClientIdPoint(clientSocket)} 已离开房间 {currentRoomId} ");

        return true;
    }

    public static async Task LeaveRoom(Socket clientSocket, bool isNormalExit)
    {
        if (!ClientRooms.ContainsKey(clientSocket))
        {
            Log($"客户端 {GetClientIdPoint(clientSocket)}  没有加入任何房间.");
            return;
        }

        try
        {
            int clientId = GetClientId(clientSocket);

            // 确保 ClientSessions 中包含该客户端的会话信息
            if (ClientSessions.ContainsKey(clientId))
            {
                var session = ClientSessions[clientId];
                session.WasNormalExit = isNormalExit;
                ClientSessions[clientId] = session;  // 更新会话数据


                // 从房间移除客户端并清理资源
                string currentRoomId = session.RoomId;
                // 确保房间存在
                if (currentRoomId != null && Rooms.ContainsKey(currentRoomId))
                {
                    Room currentRoom = Rooms[currentRoomId];
                    currentRoom.RemoveClient(clientSocket);  // 从房间移除客户端

                    // 清除客户端的房间信息
                    ClientRooms.TryRemove(clientSocket, out _);

                    // 通知其他玩家该客户端离开房间
                    await BroadcastClientJoinOrLeave(clientSocket, currentRoomId, false);  // false 表示离开房间

                    Log($"客户端 {GetClientIdPoint(clientSocket)} 已离开房间 {currentRoomId} ");
                }
                else
                {
                    Log($"房间 {currentRoomId} 不存在，无法移除客户端 {GetClientIdPoint(clientSocket)}");
                }
            }
            else
            {
                Log($"客户端 {GetClientIdPoint(clientSocket)} 不在会话中，无法执行退出操作");
            }
        }
        catch (Exception ex)
        {
            Log($"处理客户端离开房间时发生错误: {ex.Message}");
        }
    }


    public static async Task<bool> SwitchRoom(Socket clientSocket, string newRoomId)
    {
        if (!ClientRooms.ContainsKey(clientSocket))
        {
            Log($"客户端 {GetClientIdPoint(clientSocket)} 没有加入任何房间，无法切换房间。");
            return false;
        }

        string currentRoomId = ClientRooms[clientSocket];  // 记录当前房间
        var currentRoom = Rooms[currentRoomId];

        currentRoom.RemoveClient(clientSocket);  // 从当前房间移除客户端
        ClientRooms.TryRemove(clientSocket, out _);  // 移除客户端的房间记录
        await BroadcastClientJoinOrLeave(clientSocket, currentRoomId, false); // 异步广播客户端离开房间

        await Task.Delay(200);  // 等待一段时间，确保当前房间客户端移除操作完成

        await JoinRoom(clientSocket, newRoomId);  // 将客户端加入到新房间
        Log($"客户端 {GetClientIdPoint(clientSocket)} 已从房间:{currentRoomId} 切换到房间:{newRoomId}");
        return true;
    }

    public static RoomMessage HandleRoomMessage(byte[] data)
    {
        string jsonMessage = System.Text.Encoding.UTF8.GetString(data);
        RoomMessage roomMessage = JsonConvert.DeserializeObject<RoomMessage>(jsonMessage);
        roomMessage?.PrintInfo();
        return roomMessage;
    }
    public static byte[] JsonToByteArray<T>(T message) where T : ClientMessageBase
    {
        string jsonString = JsonConvert.SerializeObject(message);
        //print($"JsonToByteArray : {jsonString}");
        return Encoding.UTF8.GetBytes(jsonString);
    }
    public static T ByteArrayToJson<T>(byte[] data) where T : ClientMessageBase
    {
        string jsonString = Encoding.UTF8.GetString(data);
        T t = JsonConvert.DeserializeObject<T>(jsonString);
        //print($"ByteArrayToJson : {jsonString}");
        t.PrintInfo();
        return t;
    }

    #endregion



    #region  其他

    private static void RemoveClient(Socket clientSocket)
    {
        try
        {
            if (ClientIds.TryRemove(clientSocket, out int clientId))
            {
                var clientKey = $"{clientSocket.RemoteEndPoint}___{clientId}";
                Clients.TryRemove(clientKey, out _);
                //ClientIds.TryRemove(clientSocket, out _);
                clientSocket.Dispose();
                Log($"客户端 {clientKey} 已断开连接并清理资源。");
            }
        }
        catch (Exception ex)
        {
            Log($"移除客户端时发生异常: {ex.Message}");
        }

        //var clientEndPoint = clientSocket.RemoteEndPoint; // 提前保存
        //var clientId = ClientIds[clientSocket];

        //var clientKey = $"{GetClientIdPoint(clientSocket)}";
        //if (Clients.ContainsKey(clientKey))
        //{
        //    Clients.TryRemove(clientKey, out _);
        //    ClientIds.TryRemove(clientSocket, out _);
        //    clientSocket.Dispose();
        //    Log($"客户端 {clientKey} 已断开连接并清理资源。");
        //}
    }

    public static async Task ShutdownServer()
    {
        foreach (var kvp in Clients)
        {
            var clientKey = kvp.Key;
            var clientSocket = kvp.Value;

            try
            {
                if (clientSocket.Connected)
                {
                    clientSocket.Shutdown(SocketShutdown.Both);
                    clientSocket.Close();
                    clientSocket.Dispose();
                    Log($"客户端 {clientKey} 已关闭。");
                }
            }
            catch (Exception ex)
            {
                Log($"关闭客户端 {clientKey} 失败: {ex.Message}");
            }
        }
        Clients.Clear(); // 清理所有客户端


        serverListener.Stop();
        Log("服务器已关闭。");
    }



    public static int GetClientId(Socket clientSocket)
    {
        if (ClientIds.ContainsKey(clientSocket))
        {
            return ClientIds[clientSocket];
        }
        else
        {
            Log($"客户端ID未找到 {clientSocket.RemoteEndPoint}");
            return -1;
        }
    }
    public static string GetClientIdPoint(Socket clientSocket)
    {
        return $"{clientSocket.RemoteEndPoint}___{ClientIds[clientSocket]}";
    }
    public static string GetLocalIPAddress()
    {
        foreach (var ip in Dns.GetHostAddresses(Dns.GetHostName()))
        {
            if (ip.AddressFamily == AddressFamily.InterNetwork)
            {
                return ip.ToString();
            }
        }
        return "127.0.0.1";
    }


    public static bool TryParseNetworkMessage(byte[] message, out NetworkMessage? networkMessage)
    {
        try
        {
            if (message.Length < 4)
            {
                networkMessage = null;
                return false;
            }

            NetworkMessageType messageType = (NetworkMessageType)BitConverter.ToInt32(message, 0);
            byte[] data = new byte[message.Length - 4];
            Array.Copy(message, 4, data, 0, data.Length);

            networkMessage = new NetworkMessage(messageType, data);
            return true;
        }
        catch
        {
            networkMessage = null;
            return false;
        }
    }



    private static readonly object logLock = new object();
    public static void Log(string message)
    {
        lock (logLock)
        {
            try
            {
                string logMessage = $"[{DateTime.Now}] {message}";
                Console.WriteLine(logMessage);
                //File.AppendAllText(LogFilePath, logMessage + Environment.NewLine);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"日志输出失败: {ex.Message}");
            }
        }
    }
    #endregion

}



public class MessageWithSender
{
    public Socket SenderSocket { get; }
    public NetworkMessage Message { get; }

    public MessageWithSender(Socket senderSocket, NetworkMessage message)
    {
        SenderSocket = senderSocket;
        Message = message;
    }
}


public class CircularBuffer
{
    private readonly byte[] buffer;
    private int head;
    private int tail;
    private int count;

    public CircularBuffer(int capacity)
    {
        buffer = new byte[capacity];
        head = 0;
        tail = 0;
        count = 0;
    }

    public int Length => count;

    public void Write(byte[] data, int offset, int length)
    {
        if (length > buffer.Length - count)
            throw new InvalidOperationException("缓冲区已满，无法写入更多数据");

        for (int i = 0; i < length; i++)
        {
            buffer[tail] = data[offset + i];
            tail = (tail + 1) % buffer.Length;
        }

        count += length;
    }

    public void Read(int length, out byte[] data)
    {
        if (length > count)
            throw new InvalidOperationException("没有足够的数据可供读取");

        data = new byte[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = buffer[head];
            head = (head + 1) % buffer.Length;
        }

        count -= length;
    }

    public void Peek(int offset, int length, out byte[] data)
    {
        if (offset + length > count)
            throw new InvalidOperationException("没有足够的数据可供查看");

        data = new byte[length];
        int tempHead = head;
        for (int i = 0; i < offset; i++)
            tempHead = (tempHead + 1) % buffer.Length;

        for (int i = 0; i < length; i++)
        {
            data[i] = buffer[tempHead];
            tempHead = (tempHead + 1) % buffer.Length;
        }
    }

    public void Discard(int length)
    {
        if (length > count)
            throw new InvalidOperationException("没有足够的数据可供丢弃");

        head = (head + length) % buffer.Length;
        count -= length;
    }
}
