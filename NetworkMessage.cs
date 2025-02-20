using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;


public enum NetworkMessageType
{
    /// <summary>
    /// 位置更新
    /// </summary>
    TransformUpdate,
    /// <summary>
    /// 状态
    /// </summary>
    Status,
    /// <summary>
    /// 物体产生
    /// </summary>
    ObjectSpawn,
    /// <summary>
    /// 新客户端加入的消息
    /// </summary>
    ClientConnect,
    /// <summary>
    /// 客户端退出
    /// </summary>
    ClientDisconnect,


    JoinRoom,
    LeaveRoom,
    SwitchRoom
}


public class NetworkMessage
{

    public NetworkMessageType MessageType { get; }
    public byte[] Data { get; }

    public NetworkMessage(NetworkMessageType messageType, byte[] data)
    {
        MessageType = messageType;
        Data = data;
    }
}


/// <summary>
/// 所有消息的基类，包括客户端ID,成员类型，全局ID及物品类型。
/// </summary>
public abstract class ClientMessageBase
{
    /// <summary>
    /// 客户端 ID，唯一标识
    /// </summary>
    public int ClientId;
    /// <summary>
    /// 成员类型 1.导演端  2.裁判端  3.操作端1  4.操作端2 
    /// </summary>
    public int ClientType;
    /// <summary>
    /// 全局对象 ID，用于标识全局对象
    /// </summary>
    public int GlobalObjId;
    /// <summary>
    /// 物品类型
    /// </summary>
    public int type;


    public virtual string PrintInfo()
    {
        string info = $"客户端ID: {ClientId}, ,类型: {type}, 全局ID: {GlobalObjId}";
        Console.WriteLine(info);
        return info;
    }
}


/// <summary>
/// 房间消息 (加入.离开.切换)
/// </summary>
public class RoomMessage : ClientMessageBase
{
    /// <summary>
    /// 房间消息类型 (例子: NetworkMessageType.JoinRoom.ToString();)
    /// </summary>
    public string? roomMessageType;
    public string? roomId;

    public override string PrintInfo()
    {

        string info = base.PrintInfo() + $", 房间消息类型: {roomMessageType}, 房间id: {roomId}";
        Console.WriteLine(info);
        return info;
    }
}
