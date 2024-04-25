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
#include <thread>
#include <pthread.h>


int parse_length(std::string buf, int* idx) {
  int len = 0;

  while(buf[*idx] != '\r') {
    len = len * 10 + (buf[*idx] - '0');
    (*idx)++;
  }
  (*idx)++; // consume \n

  return len;
}

std::vector<std::string> protocol_parser(std::string buf) {
  int len = buf.size();

  std::vector<std::string> parse_result;
  std::string next_arr_el = "";

  for(int i = 0; i < len; i++) {
    if(buf[i] == '*') {
      i++;
      parse_result.resize(parse_length(buf, &i));

    } else if (buf[i] == '$') {
      int k = parse_length(buf, &i);

      int j = 0;
      while(j < k) {
        next_arr_el += tolower(buf[i + j]);
        j++;
      }

      i += j + 2; // eat CRLF

      parse_result.push_back(next_arr_el);
      next_arr_el = "";
    }
  }

  return parse_result;
}

void handle_client(int client_fd) {
  char client_command[1024] = {'\0'};

  while(recv(client_fd, client_command, sizeof(client_command), 0) > 0)
  {
    std::string string_buf {client_command};
    auto parsed_in = protocol_parser(string_buf);

    if(parsed_in[0] == "ping") {
      send(client_fd, "+pong\r\n", 7, 0);
    }

    for(int i = 0; i < sizeof(client_command); i++) client_command[i] = '\0';
  }

  close(client_fd);
}



int main(int argc, char **argv)
{
  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  std::cout << "Protocol parser test\n";
  auto test_result = protocol_parser("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
  for(auto s : test_result) std::cout << s << "\n";

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
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);

  std::vector<std::thread> threads;
  
  std::cout << "Waiting for a client to connect...\n";
  
  int client_fd;
  while(true) {
    client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    std::cout << "Client connected\n";

    threads.emplace_back(std::thread(handle_client, client_fd));
  }

  close(server_fd);

  return 0;
}
