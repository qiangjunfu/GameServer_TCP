using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

public class ClientSession
{
    public int ClientId { get; set; }
    public string? RoomId { get; set; }
    public DateTime LastActiveTime { get; set; }
    public bool WasNormalExit { get; set; } // 标识玩家是否正常退出


    public  string PrintInfo()
    {

        string info = $"会话消息 --- ClientId: {ClientId}, 客户端所在房间: {RoomId}";
        //Console.WriteLine(info);
        return info;
    }

}


