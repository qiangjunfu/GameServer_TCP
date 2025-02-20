using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;


internal class Room
{
    public string RoomId { get; }
    public HashSet<Socket> Clients { get; }

    public Room(string roomId)
    {
        RoomId = roomId;
        Clients = new HashSet<Socket>();
    }

    public void AddClient(Socket client)
    {
        Clients.Add(client);
    }

    public void RemoveClient(Socket client)
    {
        Clients.Remove(client);
    }

}

