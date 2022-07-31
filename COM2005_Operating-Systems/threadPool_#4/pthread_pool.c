/*
 * Copyright 2022. Heekuck Oh, all rights reserved
 * 이 프로그램은 한양대학교 ERICA 소프트웨어학부 재학생을 위한 교육용으로 제작되었습니다.
 */
#include <stdlib.h>
#include "pthread_pool.h"
#include <stdio.h>
/*
 * 풀에 있는 일꾼(일벌) 스레드가 수행할 함수이다.
 * FIFO 대기열에서 기다리고 있는 작업을 하나씩 꺼내서 실행한다.
 */
static void *worker(void *param)
{
	pthread_pool_t *pool = param;

	/* 
	 * pool이 활성화되어 있는 동안 동작시킨다.
	 */
    pthread_mutex_lock(&(pool->mutex));
	while(pool->running) {
        printf("thread\n");
        // queue가 비었으면 대기한다.
        if(pool->q_len == 0){
            pthread_cond_wait(&(pool->full), &(pool->mutex));
            continue;
        }
		// shutdown에 의해 깨어난 thread들은 조건을 재확인하고 lock을 풀고 종료해야 한다.
//		if(!(pool->running)){
//			pthread_mutex_unlock(&(pool->mutex));
//			pthread_exit(NULL);
//		}

		// queue에서 task를 꺼내고 queue의 정보를 수정한다.
		int deq = pool->q_front;
		task_t task = (pool->q)[deq];
		pool->q_front = (pool->q_front + 1) % pool->q_size;
		pool->q_len = pool->q_len - 1;
        
        // task 추가를 위해 대기하는 스레드 하나를 깨운다.
        pthread_cond_signal(&(pool -> empty));

		// 다른 thread에게 락을 넘겨준 뒤 task를 수행한다.
		pthread_mutex_unlock(&(pool->mutex));
		task.function(task.param);

        // 조건문 검사를 위한 lock을 획득한다.
        pthread_mutex_lock(&(pool->mutex));
	}

    // 조건문에 의해 나온 worker는 lock을 풀고 종료한다.
    pthread_mutex_unlock(&(pool->mutex));
    pthread_exit(NULL);
}

/*
 * 스레드풀을 초기화한다. 성공하면 POOL_SUCCESS를, 실패하면 POOL_FAIL을 리턴한다.
 * bee_size는 일꾼(일벌) 스레드의 갯수이고, queue_size는 작업 대기열의 크기이다.
 * 대기열의 크기 queue_size가 최소한 일꾼의 수 bee_size보다 크거나 같게 만든다.
 */
int pthread_pool_init(pthread_pool_t *pool, size_t bee_size, size_t queue_size)
{
    /* 
     * 요청한 일꾼 수가 최대 일꾼 수보다 많거나,
     * 요청한 대기열 길이가 최대 길이보다 크면 종료,
     * 대기열 길이가 일꾼 수보다 작으면 상향 조정한다.
     */
    if(bee_size > POOL_MAXBSIZE)
    	return POOL_FAIL;
    else if(queue_size > POOL_MAXQSIZE)
    	return POOL_FAIL;
    else if(queue_size < bee_size)
    	queue_size = bee_size;

    // 일꾼이 들어갈 배열을 선언하고 메모리 할당을 해준다.
    pthread_t* bees = NULL;
    bees = (pthread_t *) malloc(sizeof(pthread_t) * bee_size);
    if(bees == NULL) 
    	return POOL_FAIL;
    // task가 들어갈 배열을 선언하고 메모리 할당을 해준다.
    task_t* q = NULL;
    q = (task_t *) malloc(sizeof(task_t) * queue_size);
    if(q == NULL)
    {
    	free(bees);
    	return POOL_FAIL;
    }

    // pool의 초기값들을 넣어준다.
    (pool -> running) = true;
    (pool -> q) = q;
    (pool -> q_size) = queue_size;
    (pool -> q_front) = 0;
    (pool -> q_len) = 0;
    (pool -> bee) = bees;
    (pool -> bee_size) = bee_size;
    // mutex와 cond변수를 초기화한다.
    pthread_mutex_init(&(pool->mutex), NULL);
    pthread_cond_init(&(pool->full), NULL);
    pthread_cond_init(&(pool->empty), NULL);

    // bee_size만큼의 일꾼을 생성해 worker를 실행시킨다.
    for(int i = 0; i < bee_size; i++)
    {
    	pthread_create(bees+i, NULL, worker, pool);
    }

    return POOL_SUCCESS;

}

/*
 * 스레드풀에서 실행시킬 함수와 인자의 주소를 넘겨주며 작업을 요청한다.
 * 스레드풀의 대기열이 꽉 찬 상황에서 flag이 POOL_NOWAIT이면 즉시 POOL_FULL을 리턴한다.
 * POOL_WAIT이면 대기열에 빈 자리가 나올 때까지 기다렸다가 넣고 나온다.
 * 작업 요청이 성공하면 POOL_SUCCESS를 리턴한다.
 */
int pthread_pool_submit(pthread_pool_t *pool, void (*f)(void *p), void *p, int flag)
{
    // pool접근을 위한 lock을 획득한다.
	pthread_mutex_lock(&(pool -> mutex));

    /* 
     * queue가 꽉 찬 상황에서 POOL_NOWAIT이면 lock을 풀고 종료하고 
     * POOL_WAIT이면 대기한다.
     */
    if((pool -> q_len) == (pool -> q_size)){
    	if(flag == POOL_NOWAIT){
            pthread_mutex_unlock(&(pool -> mutex));
    		return POOL_FULL;
        }
    	else if(flag == POOL_WAIT)
    		pthread_cond_wait(&(pool -> empty), &(pool -> mutex));
    }

    // 전달받은 task를 queue에 추가하고 길이를 수정한다.
    task_t task;
    task.function = f;
    task.param = p;

    int enq = (pool->q_front + pool->q_len) % (pool -> q_size);
    (pool -> q)[enq] = task;
    (pool -> q_len) = pool->q_len + 1;

    /**
     * 다음 task를 대기하는 worker 스레드에 signal을 보내고
     * lock을 푼 뒤 종료한다.
     */
    pthread_cond_signal(&(pool -> full));
    pthread_mutex_unlock(&(pool -> mutex));
    return POOL_SUCCESS;
}

/*
 * 모든 일꾼 스레드를 종료하고 스레드풀에 할당된 자원을 모두 제거(반납)한다.
 * 락을 소유한 스레드를 중간에 철회하면 교착상태가 발생할 수 있으므로 주의한다.
 * 부모 스레드는 종료된 일꾼 스레드와 조인한 후에 할당된 메모리를 반납한다.
 * 종료가 완료되면 POOL_SUCCESS를 리턴한다.
 */
int pthread_pool_shutdown(pthread_pool_t *pool)
{
    // pool 접근을 위한 lock을 획득한다.
    pthread_mutex_lock(&(pool->mutex));

    // pool을 비활성화시킨다.
    pool->running = false;

    // worker 스레드의 개수를 미리 읽어온다.
    int bee_size = pool->bee_size;
    
    /*
     * 대기중인 스레드들을 모두 깨운 뒤
     * lock을 풀고 join하여 종료를 기다린다.
     */
    if(pthread_cond_broadcast(&(pool->full)))
        return POOL_FAIL;
    if(pthread_cond_broadcast(&(pool->empty)))
        return POOL_FAIL;

    pthread_mutex_unlock(&(pool->mutex));

    for(int i = 0; i < bee_size; i++)
    {
        pthread_join((pool->bee)[i], NULL);
    }

    /* 
     * 사용한 조건변수와 뮤텍스를 모두 지운다.
     */
    if(pthread_cond_destroy(&(pool->full)) != 0)
        return POOL_FAIL;
    if(pthread_cond_destroy(&(pool->empty)))
        return POOL_FAIL;
    if(pthread_mutex_destroy(&(pool->mutex)))
        return POOL_FAIL;

    /*
     * 할당받은 메모리를 모두 해제한다. 
     */
    free(pool->q);
    free(pool->bee);

    return POOL_SUCCESS;
}
