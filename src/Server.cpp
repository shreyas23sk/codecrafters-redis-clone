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

int64_t get_current_timestamp()
{
  return (int64_t) std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
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
  char *buf = resp_bulk.data();
  send(client_fd, buf, resp_bulk.size(), 0);
}

void handle_client(int client_fd)
{
  char client_command[1024] = {'\0'};

  while (recv(client_fd, client_command, sizeof(client_command), 0) > 0)
  {
    std::string string_buf{client_command};
    auto parsed_in = protocol_parser(string_buf);

    if (parsed_in[0] == "ping")
    {
      send_string_wrap(client_fd, "PONG");
    }
    else if (parsed_in[0] == "echo")
    {
      send_string_wrap(client_fd, parsed_in[1]);
    }
    else if (parsed_in[0] == "set")
    {
      kv[parsed_in[1]] = parsed_in[2];

      if(parsed_in.size() > 3) {
        if(parsed_in[3] == "px") {
          valid_until_ts[parsed_in[1]] = get_current_timestamp() + stoi(parsed_in[4]);
        }
      }

      send_string_wrap(client_fd, "OK");
    }
    else if (parsed_in[0] == "get")
    {
      if (!kv.contains(parsed_in[1]) || (valid_until_ts.contains(parsed_in[1]) && get_current_timestamp() > valid_until_ts[parsed_in[0]]))
        send_string_wrap(client_fd, "");
      else
        send_string_wrap(client_fd, kv[parsed_in[1]]);
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

  std::cout << "Protocol parser test\n";
  auto test_result = protocol_parser("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
  for (auto s : test_result)
    std::cout << s << "\n";

  // Uncomment this block to pass the first stage
  //
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0)
  {
    std::cerr << "Failed to create server socket\n";
    return 1;
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
  server_addr.sin_port = htons(6379);

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
