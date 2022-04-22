#include <stdio.h>
#include <sys/types.h>   // definitions of a number of data types used in socket.h and netinet/in.h
#include <sys/socket.h>  // definitions of structures needed for sockets, e.g. sockaddr
#include <netinet/in.h>  // constants and structures needed for internet domain addresses, e.g. sockaddr_in
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <ctype.h>

#include <fcntl.h>
#include <sys/stat.h>

#define BUF_SIZE 1024

/* Function that decodes encoded file names */
void form_fileName(char * name)
{
    int idx = 0;
    char * tmp_name;
    tmp_name = malloc(strlen(name));
    
    for(int i = 0; i < strlen(name); i++){
        if(name[i] == '+')
        {
            tmp_name[idx++] = ' ';   /* Processing blank characters */
        }
        else if(name[i] != '%')
        {
            tmp_name[idx++] = name[i];
        }
        else if(!isxdigit(name[i+1]) || !isxdigit(name[i+2]))
        {
            tmp_name[idx++] = name[i];
        }
        else{   /* Processing ASCII characters */
            char first_char = tolower(name[i+1]);
            char second_char = tolower(name[i+2]);
            
            if(first_char <= '9'){
              first_char = first_char - '0';
            }
            else{
              first_char = first_char - 'a' + 10;
            }
            if(second_char <= '9'){
              second_char = second_char - '0';
            }
            else{
              second_char = second_char - 'a' + 10;
            }
            
            tmp_name[idx++] = (16 * first_char + second_char);
            i += 2;
        }
    }
    tmp_name[idx] = '\0';
    strcpy(name, tmp_name);
}

int main(int argc, char *argv[])
{
    int sockfd, newsockfd;
    int port_no;
    
    socklen_t cli_len;
    
    struct sockaddr_in serv_addr, cli_addr;
    
    if (argc < 2)   /* not enter port number */
    {
        perror("no port argument");
        exit(1);
    }
    
    /* Create a new socket */
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) /* socket open error */
    {
        perror("ERROR openning socket");
        exit(1);
    }
    
    memset((char *) &serv_addr, 0, sizeof(serv_addr));
    port_no = atoi(argv[1]); //atoi converts from String to Integer
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY; //for the server the IP address is always the address that the server is running on
    serv_addr.sin_port = htons(port_no); //convert from host to network byte order
    
    /* Bind the socket to the server address */
    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
    {
        perror("ERROR on bind");
        exit(1);
    }
    
    /* Listen for socket connections */
    listen(sockfd,5);
    
    // 클라이언트 연결.
    while (1)
    {
        /* Accept connected client */
        char buffer[BUF_SIZE];
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &cli_len);
        read(newsockfd, buffer, BUF_SIZE);
        printf("\n%s\n", buffer);
        
        if(strncmp("GET", buffer, 3) != 0){
            /* no GET request message */
        }
        else
        {
            char * response_header = NULL;
            char file[BUF_SIZE];
            
            /* find file name in request message */
            int i, j=0;
            for(i = 5; i<strlen(buffer); i++)
            {
              if(buffer[i] == ' ')
              {
                break;
              }
              file[j++] = buffer[i];
            }
            file[j] = '\0';
            
            /* edit (encoding) file name */
            form_fileName(file);
            
            if(strlen(file) == 0)
            {
                /* no request file name */
            }
            else
            {
                int fd;
                
                /* create file path */
                char file_path[BUF_SIZE];
                strcpy(file_path, "./");
                strcat(file_path, file);
                
                /* file open */
                if((fd = open(file_path, O_RDONLY)) == -1){
                    /* files that do not exist */
                }
                else
                {
                    struct stat sb;
                    fstat(fd, &sb);
                    
                    /* (1) header part */
                    char type[32];  // store file type
                    
                    for (int i = 0; i < strlen(file); i++){
                        if(file[i] == '.'){
                            strcpy(type, &file[i]);
                        }
                    }
                    if(strcmp(type, ".html")==0){
                        strcpy(type, "text/html");
                    }
                    else if(strcmp(type, ".gif")==0){
                        strcpy(type, "image/gif");
                    }
                    else if(strcmp(type, ".jpeg")==0){
                        strcpy(type, "image/jpeg");
                    }
                    else if(strcmp(type, ".mp3")==0){
                        strcpy(type, "audio/mpeg");
                    }
                    else if(strcmp(type, ".pdf")==0){
                        strcpy(type, "application/pdf");
                    }
                    
                    size_t size = snprintf(NULL, 0,
                                           "HTTP/1.1 200 OK\r\n"
                                           "Content-Length: %ld\r\n"
                                           "Content-Type: %s\r\n"
                                           "Accept-Ranges: bytes\r\n"
                                           "Connection: close\r\n"
                                           "\r\n"
                                           , sb.st_size, type) + 1;
                    response_header = malloc(size);
                    sprintf(response_header,
                            "HTTP/1.1 200 OK\r\n"
                            "Content-Length: %ld\r\n"
                            "Content-Type: %s\r\n"
                            "Accept-Ranges: bytes\r\n"
                            "Connection: close\r\n"
                             "\r\n"
                            , sb.st_size, type);
                    
                    /* write data */
                    write(newsockfd, response_header, strlen(response_header));
                    free(response_header);
                    
                    
                    /* (2) file data part*/
                    int n;
                    char fbuf[BUF_SIZE];
                    /* read file data */
                    while((n=read(fd,fbuf,sizeof(fbuf))) > 0){
                        /* write data */
                        write(newsockfd, fbuf, n);
                        memset(fbuf, 0, sizeof(fbuf));
                    }
                    close(fd);
                }
            }
        }
        close(newsockfd);
    }
}
