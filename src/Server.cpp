#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <vector>
#include <map>
#include <chrono>
#include <thread>
#include <pthread.h>

std::map<std::string, std::string> kv;
std::map<std::string, int64_t> valid_until_ts;

std::string hex_empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"; 

int master_port = -1; // -1 -> master
std::string master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
int master_repl_offset = 0; 

int64_t get_current_timestamp()
{
  return (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

std::string hex_to_bin(std::string hex_str) 
{
  std::string res = "";
  
  for(int i = 0; i < hex_str.size(); i++)
  {
    switch(hex_str[i])
    {
      case '0':
        res += "0000";
        break;
      case '1':
        res += "0001";
        break;  
      case '2':
        res += "0010";
        break;
      case '3':
        res += "0011";
        break;
      case '4':
        res += "0100";
        break;
      case '5':
        res += "0101";
        break;
      case '6':
        res += "0110";
        break;
      case '7':
        res += "0111";
        break;
      case '8':
        res += "1000";
        break;
      case '9':
        res += "1001";
        break;
      case 'a':
        res += "1010";
        break;
      case 'b':
        res += "1011";
        break;
      case 'c':
        res += "1100";
        break;
      case 'd':
        res += "1101";
        break;
      case 'e':
        res += "1110";
        break;
      case 'f':
        res += "1111";
        break;
    }
  }

  return res;
}

int parse_length(std::string buf, int *idx)
{
  int len = 0;

  while (buf[*idx] != '\r')
  {
    len = len * 10 + (buf[*idx] - '0');
    (*idx)++;
  }
  (*idx)++; // consume \n

  return len;
}

std::vector<std::string> input_tokenizer(std::string buf)
{
  std::vector<std::string> in_tokens;

  std::string curr_arr_el = "";
  for (int i = 0; i < buf.size(); i++)
  {
    if (buf[i] != '\r' && buf[i] != '\n')
      curr_arr_el += buf[i];
    else if (buf[i] == '\n')
    {
      in_tokens.push_back(curr_arr_el);
      curr_arr_el = "";
    }
  }

  return in_tokens;
}

std::vector<std::string> protocol_parser(std::string buf)
{
  int len = buf.size();

  std::vector<std::string> parse_result;

  std::vector<std::string> in_tokens = input_tokenizer(buf);

  int total_no_of_out_tokens;
  int max_size_of_next_token;
  for (auto s : in_tokens)
  {
    if (s[0] == '*')
      total_no_of_out_tokens = stoi(s.substr(1));
    else if (s[0] == '$')
      max_size_of_next_token = stoi(s.substr(1));
    else
    {
      if (s.size() != max_size_of_next_token)
        std::cerr << "Invalid command\n";
      parse_result.push_back(s);
    }
  }

  if (parse_result.size() != total_no_of_out_tokens)
    std::cerr << "Invalid command\n";

  return parse_result;
}

std::string token_to_resp_bulk(std::string token)
{
  if (token.empty())
    return "$-1\r\n";
  return "$" + std::to_string(token.size()) + "\r\n" + token + "\r\n";
}

void send_string_wrap(int client_fd, std::string msg)
{
  std::string resp_bulk = token_to_resp_bulk(msg);
  // std::cout << resp_bulk << "\n";
  char *buf = resp_bulk.data();
  send(client_fd, buf, resp_bulk.size(), 0);
}

void send_string_vector_wrap(int client_fd, std::vector<std::string> msgs) 
{
  std::string combined_resp = "*" + std::to_string(msgs.size()) + "\r\n";

  for(std::string str : msgs) 
  {
    combined_resp += token_to_resp_bulk(str);
  }

  char *buf = combined_resp.data();
  send(client_fd, buf, combined_resp.size(), 0);
}

void send_rdb_file_data(int client_fd, std::string hex) 
{
  std::string bin = hex_to_bin(hex);
  std::string resp = "$" + std::to_string(hex.size()/2) + "\r\n" + bin;
  std::cout << resp << "\n";
  char* buf = resp.data();
  send(client_fd, buf, resp.size(), 0);
}


void handle_client(int client_fd)
{
  char client_command[1024] = {'\0'};

  while (recv(client_fd, client_command, sizeof(client_command), 0) > 0)
  {
    std::string string_buf{client_command};

    for(int i = 0; i < string_buf.size(); i++) string_buf[i] = tolower(string_buf[i]);
    auto parsed_in = protocol_parser(string_buf);

    std::string command = parsed_in[0];

    if (command == "ping")
    {
      send_string_wrap(client_fd, "PONG");
    }
    else if (command == "echo")
    {
      send_string_wrap(client_fd, parsed_in[1]);
    }
    else if (command == "set")
    {
      std::string key = parsed_in[1];
      std::string value = parsed_in[2];

      kv[key] = value;

      if (parsed_in.size() > 3)
      {
        if (parsed_in[3] == "px")
        {
          valid_until_ts[key] = get_current_timestamp() + (int64_t)stoi(parsed_in[4]);
        }
      }

      send_string_wrap(client_fd, "OK");
    }
    else if (command == "get")
    {
      std::string key = parsed_in[1];

      if (!kv.contains(key) || (valid_until_ts.contains(key) && get_current_timestamp() > valid_until_ts[key]))
        send_string_wrap(client_fd, "");
      else
        send_string_wrap(client_fd, kv[key]);
    }
    else if (command == "info") 
    {
      int args = parsed_in.size() - 1;

      if(args == 1 && parsed_in[1] == "replication")
      {
        if(master_port == -1) 
        {
          std::string resp = "role:master\r\nmaster_replid:" + master_repl_id + "\r\nmaster_repl_offset:" + std::to_string(master_repl_offset);
        
          send_string_wrap(client_fd, resp);
        }
        else 
          send_string_wrap(client_fd, "role:slave");
      }
    }
    else if (command == "replconf") 
    {
      send(client_fd, "+OK\r\n", 5, 0);
    }
    else if (command == "psync") 
    {
      std::string recv_master_id = parsed_in[1];
      int recv_master_offset = stoi(parsed_in[2]);

      if(recv_master_id == "?" && recv_master_offset == -1) 
      {
        std::string resp = "+FULLRESYNC " + master_repl_id + " " + std::to_string(master_repl_offset) + "\r\n";
        send(client_fd, resp.data(), resp.size(), 0);
        send_string_wrap(client_fd, hex_to_bin(hex_empty_rdb));
        send_rdb_file_data(client_fd, hex_empty_rdb);
      }

    }

    for (int i = 0; i < sizeof(client_command); i++)
      client_command[i] = '\0';
  }

  close(client_fd);
}

int main(int argc, char **argv)
{
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  // std::cout << "Protocol parser test\n";
  // auto test_result = protocol_parser("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
  // for (auto s : test_result)
  //   std::cout << s << "\n";

  int self_port = 6379;
  if(argc > 1) 
  {
    if(strcmp(argv[1], "--port") == 0) 
    {
      std::string port_in {argv[2]};
      self_port = stoi(port_in);
    }
  }

  if(argc > 3) 
  {
    int port_idx = 4;
    if(strcmp(argv[4], "localhost") == 0) 
      port_idx = 5;
    
    if(strcmp(argv[3], "--replicaof") == 0)
    {
      std::string master {argv[port_idx]};
      master_port = stoi(master);
    }

  }

  // Uncomment this block to pass the first stage
  //
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0)
  {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  if(master_port != -1) 
  {
    int replica_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in master_addr;
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(master_port);
    master_addr.sin_addr.s_addr = INADDR_ANY; 

    if(connect(replica_fd, (struct sockaddr *) &master_addr, sizeof(master_addr)) == -1) 
    {
      std::cerr << "Replica failed to connect to master\n";
    }
    
    char buf[1024] = {'\0'};

    //send_string_vector_wrap(replica_fd, {"ping"});
    recv(replica_fd, buf, sizeof(buf), 0);
    memset(buf, 0, 1024);

    send_string_vector_wrap(replica_fd, {"REPLCONF", "listening-port", std::to_string(self_port)});
    recv(replica_fd, buf, sizeof(buf), 0);
    memset(buf, 0, 1024);
    
    send_string_vector_wrap(replica_fd, {"REPLCONF", "capa", "psync2"});
    recv(replica_fd, buf, sizeof(buf), 0);
    memset(buf, 0, 1024);

    send_string_vector_wrap(replica_fd, {"PSYNC", "?", "-1"});
    recv(replica_fd, buf, sizeof(buf), 0);
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
  {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(self_port);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0)
  {
    std::cerr << "listen failed\n";
    return 1;
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);

  std::vector<std::thread> threads;

  std::cout << "Waiting for a client to connect...\n";

  int client_fd;
  while (true)
  {
    client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
    std::cout << "Client connected\n";

    threads.emplace_back(std::thread(handle_client, client_fd));
  }

  close(server_fd);

  return 0;
}
