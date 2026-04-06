/*
Copyright (c) 2009-2021 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License 2.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   https://www.eclipse.org/legal/epl-2.0/
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause

Contributors:
   Roger Light - initial implementation and documentation.
   Tatsuzo Osawa - Add epoll.
*/

#include "config.h"

#ifdef WITH_EPOLL

#ifndef WIN32
#  define _GNU_SOURCE
#endif

#ifndef WIN32
#  include <sys/epoll.h>
#  define MAX_EVENTS 1000
#endif

#include <errno.h>
#include <signal.h>

#include "mosquitto_broker_internal.h"
#include "mux.h"
#include "packet_mosq.h"
#include "util_mosq.h"

static void loop_handle_reads_writes(struct mosquitto *context, uint32_t events);

static struct epoll_event ep_events[MAX_EVENTS];

enum packet_prio {
	PRIO_HIGH = 0,
	PRIO_MID,
	PRIO_LOW,
	PRIO_NORMAL
};

static enum packet_prio classify_topic_from_peek(struct mosquitto *context);

int mux_epoll__init(void)
{
	memset(&ep_events, 0, sizeof(struct epoll_event)*MAX_EVENTS);

	db.epollfd = 0;
	if((db.epollfd = epoll_create(MAX_EVENTS)) == -1){
		log__printf(NULL, MOSQ_LOG_ERR, "Error in epoll creating: %s", strerror(errno));
		return MOSQ_ERR_UNKNOWN;
	}

	return MOSQ_ERR_SUCCESS;
}


int mux_epoll__add_listeners(struct mosquitto__listener_sock *listensock, int listensock_count)
{
	for(int i=0; i<listensock_count; i++){
		struct epoll_event ev;

		memset(&ev, 0, sizeof(struct epoll_event));
		ev.data.ptr = &listensock[i];
		ev.events = EPOLLIN;
		if(epoll_ctl(db.epollfd, EPOLL_CTL_ADD, listensock[i].sock, &ev) == -1){
			log__printf(NULL, MOSQ_LOG_ERR, "Error in epoll initial registering: %s", strerror(errno));
			return MOSQ_ERR_UNKNOWN;
		}
	}

	return MOSQ_ERR_SUCCESS;
}


int mux_epoll__delete_listeners(struct mosquitto__listener_sock *listensock, int listensock_count)
{
	for(int i=0; i<listensock_count; i++){
		if(epoll_ctl(db.epollfd, EPOLL_CTL_DEL, listensock[i].sock, NULL) == -1){
			return MOSQ_ERR_UNKNOWN;
		}
	}

	return MOSQ_ERR_SUCCESS;
}


int mux_epoll__add_out(struct mosquitto *context)
{
	if(!(context->events & EPOLLOUT)){
		struct epoll_event ev;

		memset(&ev, 0, sizeof(struct epoll_event));
		ev.data.ptr = context;
		ev.events = EPOLLIN | EPOLLOUT;
		if(epoll_ctl(db.epollfd, EPOLL_CTL_MOD, context->sock, &ev) == -1){
			if((errno != ENOENT)||(epoll_ctl(db.epollfd, EPOLL_CTL_ADD, context->sock, &ev) == -1)){
				log__printf(NULL, MOSQ_LOG_DEBUG, "Error in epoll re-registering to EPOLLOUT: %s", strerror(errno));
			}
		}
		context->events = EPOLLIN | EPOLLOUT;
	}
	return MOSQ_ERR_SUCCESS;
}


int mux_epoll__remove_out(struct mosquitto *context)
{
	if(context->events & EPOLLOUT){
		struct epoll_event ev;

		memset(&ev, 0, sizeof(struct epoll_event));
		ev.data.ptr = context;
		ev.events = EPOLLIN;
		if(epoll_ctl(db.epollfd, EPOLL_CTL_MOD, context->sock, &ev) == -1){
			if((errno != ENOENT)||(epoll_ctl(db.epollfd, EPOLL_CTL_ADD, context->sock, &ev) == -1)){
				log__printf(NULL, MOSQ_LOG_DEBUG, "Error in epoll re-registering to EPOLLIN: %s", strerror(errno));
			}
		}
		context->events = EPOLLIN;
	}
	return MOSQ_ERR_SUCCESS;
}


int mux_epoll__new(struct mosquitto *context)
{
	struct epoll_event ev;

	memset(&ev, 0, sizeof(struct epoll_event));
	ev.events = EPOLLIN;
	ev.data.ptr = context;
	if(epoll_ctl(db.epollfd, EPOLL_CTL_ADD, context->sock, &ev) == -1){
		if(errno != EEXIST){
			log__printf(NULL, MOSQ_LOG_ERR, "Error in epoll accepting: %s", strerror(errno));
		}
	}
	context->events = EPOLLIN;
	return MOSQ_ERR_SUCCESS;
}


int mux_epoll__delete(struct mosquitto *context)
{
	if(context->sock != INVALID_SOCKET){
		if(epoll_ctl(db.epollfd, EPOLL_CTL_DEL, context->sock, NULL) == -1){
			return 1;
		}
	}
	return 0;
}


int mux_epoll__handle(void)
{
	struct epoll_event ev;
	struct mosquitto *context;
	struct mosquitto__listener_sock *listensock;
	int event_count;

	memset(&ev, 0, sizeof(struct epoll_event));
#if defined(WITH_WEBSOCKETS)
	event_count = epoll_wait(db.epollfd, ep_events, MAX_EVENTS, 100);
#else
	event_count = epoll_wait(db.epollfd, ep_events, MAX_EVENTS, db.next_event_ms);
#endif

	db.now_s = mosquitto_time();
	db.now_real_s = time(NULL);

	switch(event_count){
		case -1:
			if(errno != EINTR){
				log__printf(NULL, MOSQ_LOG_ERR, "Error in epoll waiting: %s.", strerror(errno));
			}
			break;
		case 0:
			break;
		default:
			for(int i=0; i<event_count; i++){
				context = ep_events[i].data.ptr;
				if(context->ident == id_listener){
					listensock = ep_events[i].data.ptr;

					if(ep_events[i].events & (EPOLLIN | EPOLLPRI)){
						while((context = net__socket_accept(listensock)) != NULL){
						}
					}
#if defined(WITH_WEBSOCKETS) && WITH_WEBSOCKETS == WS_IS_LWS
				}else if(context->ident == id_listener_ws){
					/* Nothing needs to happen here, because we always call lws_service in the loop.
					 * The important point is we've been woken up for this listener. */
#endif
				}
			}

			/* 2. client 이벤트 분류 */
			for(int i=0; i<event_count && i<64; i++){
				context = ep_events[i].data.ptr;

				if(context->ident != id_client){
					continue;
				}

				/* 읽기 이벤트가 없으면 일반 우선순위 */
				if(!(ep_events[i].events & EPOLLIN)){
					normal_mask |= (1ULL << i);
					continue;
				}

				switch(classify_topic_from_peek(context)){
					case PRIO_HIGH:
						high_mask |= (1ULL << i);
						break;

					case PRIO_MID:
						mid_mask |= (1ULL << i);
						break;

					case PRIO_LOW:
						low_mask |= (1ULL << i);
						break;

					case PRIO_NORMAL:
					default:
						normal_mask |= (1ULL << i);
						break;
				}
			}

			/* 3. 우선순위 순서대로 처리 */
			while(1){
				int idx;

				if(high_mask != 0){
					idx = __builtin_ctzll(high_mask);
					context = ep_events[idx].data.ptr;
					loop_handle_reads_writes(context, ep_events[idx].events);
					high_mask &= ~(1ULL << idx);

				}else if(mid_mask != 0){
					idx = __builtin_ctzll(mid_mask);
					context = ep_events[idx].data.ptr;
					loop_handle_reads_writes(context, ep_events[idx].events);
					mid_mask &= ~(1ULL << idx);

				}else if(low_mask != 0){
					idx = __builtin_ctzll(low_mask);
					context = ep_events[idx].data.ptr;
					loop_handle_reads_writes(context, ep_events[idx].events);
					low_mask &= ~(1ULL << idx);

				}else if(normal_mask != 0){
					idx = __builtin_ctzll(normal_mask);
					context = ep_events[idx].data.ptr;
					loop_handle_reads_writes(context, ep_events[idx].events);
					normal_mask &= ~(1ULL << idx);

				}else{
					break;
				}
			}
			break;
	}
	return MOSQ_ERR_SUCCESS;
}


int mux_epoll__cleanup(void)
{
	(void)close(db.epollfd);
	db.epollfd = 0;
	return MOSQ_ERR_SUCCESS;
}

static enum packet_prio classify_topic_from_peek(struct mosquitto *context)
{
	unsigned char buf[256];
	ssize_t n;
	size_t pos;
	size_t rem_len_bytes;
	uint32_t remaining_length;
	uint32_t multiplier;
	uint16_t topic_len;
	char topic[128];
	size_t len;

	if(!context || context->sock == INVALID_SOCKET){
		return PRIO_NORMAL;
	}

	memset(buf, 0, sizeof(buf));
	n = recv(context->sock, buf, sizeof(buf), MSG_PEEK);
	if(n <= 0){
		return PRIO_NORMAL;
	}

	/* 최소 fixed header 2바이트는 있어야 함 */
	if(n < 2){
		return PRIO_NORMAL;
	}

	/* MQTT Control Packet Type = upper nibble
	 * PUBLISH = 0x3
	 */
	if(((buf[0] >> 4) & 0x0F) != 0x03){
		return PRIO_NORMAL;
	}

	/* Remaining Length 파싱 */
	pos = 1;
	remaining_length = 0;
	multiplier = 1;
	rem_len_bytes = 0;

	do{
		if(pos >= (size_t)n){
			return PRIO_NORMAL;
		}

		remaining_length += (buf[pos] & 0x7F) * multiplier;
		multiplier *= 128;
		rem_len_bytes++;
	}while((buf[pos++] & 0x80) != 0 && rem_len_bytes < 4);

	/* topic length 2바이트 확인 */
	if(pos + 2 > (size_t)n){
		return PRIO_NORMAL;
	}

	topic_len = (uint16_t)((buf[pos] << 8) | buf[pos+1]);
	pos += 2;

	if(topic_len == 0){
		return PRIO_NORMAL;
	}

	/* topic 문자열 전체가 peek buffer 안에 있는지 확인 */
	if(pos + topic_len > (size_t)n){
		return PRIO_NORMAL;
	}

	/* local topic buffer 크기 제한 */
	if(topic_len >= sizeof(topic)){
		return PRIO_NORMAL;
	}

	memcpy(topic, &buf[pos], topic_len);
	topic[topic_len] = '\0';
	len = strlen(topic);

	if(len >= 7 && strcmp(topic + len - 7, "/pQoS0") == 0){
		return PRIO_HIGH;
	}else if(len >= 7 && strcmp(topic + len - 7, "/pQoS1") == 0){
		return PRIO_MID;
	}else if(len >= 7 && strcmp(topic + len - 7, "/pQoS2") == 0){
		return PRIO_LOW;
	}else{
		return PRIO_NORMAL;
	}
}

static void loop_handle_reads_writes(struct mosquitto *context, uint32_t events)
{
	int err;
	socklen_t len;
	int rc;

	if(context->sock == INVALID_SOCKET){
		return;
	}

#if defined(WITH_WEBSOCKETS) && WITH_WEBSOCKETS == WS_IS_LWS
	if(context->wsi){
		struct lws_pollfd wspoll;
		wspoll.fd = context->sock;
		wspoll.events = (int16_t)context->events;
		wspoll.revents = (int16_t)events;
		lws_service_fd(lws_get_context(context->wsi), &wspoll);
		return;
	}
#endif

	if(events & EPOLLOUT
#ifdef WITH_TLS
			|| context->want_write
			|| (context->ssl && context->state == mosq_cs_new)
#endif
			){

		if(context->state == mosq_cs_connect_pending){
			len = sizeof(int);
			if(!getsockopt(context->sock, SOL_SOCKET, SO_ERROR, (char *)&err, &len)){
				if(err == 0){
					mosquitto__set_state(context, mosq_cs_new);
#if defined(WITH_ADNS) && defined(WITH_BRIDGE)
					if(context->bridge){
						bridge__connect_step3(context);
					}
#endif
				}
			}else{
				do_disconnect(context, MOSQ_ERR_CONN_LOST);
				return;
			}
		}
		rc = packet__write(context);
		if(rc){
			do_disconnect(context, rc);
			return;
		}
	}

	if(events & EPOLLIN
#ifdef WITH_TLS
			|| (context->ssl && context->state == mosq_cs_new)
#endif
			){

		do{
			switch(context->transport){
				case mosq_t_tcp:
				case mosq_t_ws:
					rc = packet__read(context);
					break;
#if defined(WITH_WEBSOCKETS) && WITH_WEBSOCKETS == WS_IS_BUILTIN
				case mosq_t_http:
					rc = http__read(context);
					break;
#endif
#if !defined(WITH_WEBSOCKETS) || WITH_WEBSOCKETS == WS_IS_BUILTIN
				/* Not supported with LWS */
				case mosq_t_proxy_v2:
					rc = proxy_v2__read(context);
					break;
				case mosq_t_proxy_v1:
					rc = proxy_v1__read(context);
					break;
#endif
				default:
					rc = MOSQ_ERR_INVAL;
					break;
			}
			if(rc){
				do_disconnect(context, rc);
				return;
			}
		}while(SSL_DATA_PENDING(context));
	}else{
		if(events & (EPOLLERR | EPOLLHUP)){
			do_disconnect(context, MOSQ_ERR_CONN_LOST);
			return;
		}
	}
}
#endif
