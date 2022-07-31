#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>

#define MAX_LINE 80 /* 한 줄 명령문의 최대 길이 */
#define READ_END 0
#define WRITE_END 1


/* 명령어 파싱 함수 */
void tokenize_cmd(char *line, char *args[])
{
    char *token = (char *)malloc(MAX_LINE * sizeof(char));  /* 공백 단위인 token을 저장할 char포인터 */
    int token_idx = 0;  /* token 개수에 관한 index */
    int char_idx = 0;   /* token 내의 char 개수에 관한 index */

    for (int i = 0; i < strlen(line); i++) {
        char c = line[i];
        if (c == ' ' || c == '\n' || c == '\0') {    /* 공백 문자일때 */
            if (char_idx != 0) {    /* token에 아무 것도 없지 않을때 */
                token[char_idx] = '\0';
                args[token_idx] = (char*)malloc(MAX_LINE * sizeof(char));   /* 다음 token을 저장하기 위한 char포인터 공간 할당 */
                strcpy(args[token_idx++], token);   /* token을 args에 저장 */
                char_idx = 0;   /* 새로운 token을 위해 idx 초기화 */
            }
        }
        else {  /* 공백 문자가 아닐때 */
            token[char_idx++] = c;  /* 해당 문자를 token에 저장 */
        }
    }
    free(token);
    args[token_idx] = NULL; /* args의 끝에 NULL 추가 */
}



/* 파이프 명령어 실행 함수 */
void pipe_cmd(char **args, int point_idx)
{
    int fd[2];
    int pid1;
    int pid2;
    int status;
    
    pipe(fd);    /* pipe create */
    
    if ((pid1 = fork()) < 0) {
        fprintf(stderr, "Fork failed");
        exit(1);
    }
    else if (pid1 == 0) {
        close(fd[READ_END]);    /* READ pipe close */
        dup2(fd[WRITE_END], STDOUT_FILENO); /* standard output이 fd에 복사됨 */
        close(fd[WRITE_END]);   /* WRITE pipe close */
        
        execvp(args[0], &args[0]);  /* 명령어1 실행 */
    }
    else {
        if ((pid2 = fork()) < 0) {
            fprintf(stderr, "Fork failed");
            exit(1);
        }
        else if (pid2 == 0) {
            close(fd[WRITE_END]);   /* WRITE pipe close */
            dup2(fd[READ_END], STDIN_FILENO);   /* standard input에 대해서 fd에 복사됨 */
            close(fd[READ_END]);    /* READ pipe close */
            
            execvp(args[point_idx+1], &args[point_idx+1]);  /* 명령어2 실행 */
        }
        else {
            close(fd[READ_END]);
            close(fd[WRITE_END]);
            wait(NULL);
        }
    }
    return;
}



/* 리다이렉션 명령어 실행 함수 */
void redirection_cmd(char **args, int point_idx, int is_left_redireciton)
{
    int fd;
    int pid;
    int status;
    
    if ((pid = fork()) < 0) {               /* fork */
        fprintf(stderr, "Fork failed");
        exit(1);
    }
    else if (pid == 0) {                    /* 자식 프로세스인 경우 */
        fd = open(args[point_idx+1], O_RDWR | O_CREAT);  /* 읽기, 쓰기가 가능한 파일을 열기 */
        if (fd < 0) { perror ("Open error"); exit(-1); }

        is_left_redireciton ? dup2(fd, STDIN_FILENO) : dup2(fd, STDOUT_FILENO); /* '<' - standard input이 fd에 복사됨, '>' - 모든 standard output 해당 fd로 복사됨 */
        close(fd);  /* pipe close */
        execvp(args[0], &args[0]);
    }
    wait(&status);
    if (WIFSIGNALED(status)) {
        exit(1);
    }
    return ;
}



/* 명령어를 실행하는 함수 */
void exec_cmd(char **args)
{
    int has_pipe = 0;               /* 파이프 유무 플래그 */
    int has_redirection = 0;        /* 리다이렉션 유무 플래그 */
    int is_left_redireciton = 0;    /* 리다이렉션 방향 플래그 */
    int point_idx = 0;              /* 파이프 또는 리다이렉션 위치 */
    
    
    for (int i = 0; args[i] != NULL; i++) {
        if (strcmp(args[i], "|") == 0){    /* 파이프가 있다면 */
            args[i] = NULL;             /* | -> NULL로 수정, index 저장 */
            point_idx = i;
            has_pipe = 1;
        }
        else if (strcmp(args[i], "<") == 0 || strcmp(args[i], ">") == 0) {  /* 리다이렉션이 있다면 */
            is_left_redireciton = strcmp(args[i], ">") ? 1 : 0;     /* '<': 1, '>': 0 */
            args[i] = NULL;                                         /* </> -> NULL로 수정, index 저장 */
            point_idx = i;
            has_redirection = 1;
        }
    }
    
    if (has_pipe) { /* 파이프가 있다면 */
        pipe_cmd(args, point_idx);
    }
    else if (has_redirection) { /* 리다이렉션이 있다면 */
        redirection_cmd(args, point_idx, is_left_redireciton);
    }
    else{  /* 파이프, 리다이렉션이 없을때 */
        execvp(args[0], args);
        
        int status;
        wait(&status);
        if (WIFSIGNALED(status)) {
            exit(1);
        }
    }
    exit(0);
}



int main(void)
{
    char  line[MAX_LINE];                   /* 명령어를 저장할 배열 */
    char *args[MAX_LINE/2 + 1];             /* 명령어를 인수 단위로 저장할 배열 */
    int should_run = 1;                     /* 프로그램의 종료를 나타낼 플래그 */
    int has_bg = 0;                         /* '&'가 포함되어 있는지 나타낼 플래그 */
    int i = 0;
    
    while (should_run) {
        printf("osh>");
        
        fflush(stdout);                     /* 출력 버퍼를 비운다 */
        memset(line, 0, sizeof(line));      /* line 내에 남아 있는 데이터를 비운다 */
        
        
        scanf("%[^\n]", line);              /* 사용자에게 명령어를 입력 받는다 */
        getchar();
        line[strlen(line)] = '\n';
        
        tokenize_cmd(line, args);           /* 받은 명령어를 파싱한다 */
        
        if (!args[0]) {                     /* 입력이 없을때 */
            continue;
        }
        
        if (strcmp(args[0], "exit") == 0) {     /* exit 명령을 받았을때 */
            should_run = 0;
            return 0;
        }
        
        while(args[i] != NULL) {
            i++;
        }
        if (strcmp(args[i-1], "&") == 0) {        /* '&'이 명령어에 포함되어 있는지 확인 */
            args[i-1] = NULL;
            has_bg = 1;
        }
        
        
        int pid;
        if ((pid = fork()) < 0) {           /* fork */
            fprintf(stderr, "Fork failed");
            return 1;
        }
        else if (pid == 0) {                /* 자식 프로세스인 경우 */
            exec_cmd(args);
        }
        else {                              /* 부모 프로세스인 경우 */
            if(!has_bg) {
                int status;
                waitpid(pid, &status, 0);   /* 백그라운드 실행이 아닐때, 실행이 끝나기를 기다림 */
                if (WIFSIGNALED(status)) {
                    fprintf(stderr, "Child exited by signal : %d\n", WTERMSIG(status));
                }
            }
            memset(args, 0, sizeof(args));
        }
    }
    
    return 0;
}
 
