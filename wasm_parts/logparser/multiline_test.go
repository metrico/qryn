package logparser

import (
	"context"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func writeByLine(m *MultilineCollector, data string, ts time.Time) {
	for i, line := range strings.Split(data, "\n") {
		m.Add(LogEntry{Timestamp: ts.Add(time.Millisecond * time.Duration(i)), Content: line, Level: LevelUnknown})
	}
}

func TestMultilineCollector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	m := NewMultilineCollector(ctx, 10*time.Millisecond)
	defer cancel()

	tracebackStr := `2020-03-20 08:48:57,067 ERROR [django.request:222] log 46 140452532862280 Internal Server Error: /article
	Traceback (most recent call last):
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/base/base.py", line 220, in ensure_connection
	   self.connect()
	 File "/usr/local/lib/python3.8/site-packages/django/utils/asyncio.py", line 26, in inner
	   return func(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/base/base.py", line 197, in connect
	   self.connection = self.get_new_connection(conn_params)
	 File "/usr/local/lib/python3.8/site-packages/django_prometheus/db/common.py", line 44, in get_new_connection
	   return super(DatabaseWrapperMixin, self).get_new_connection(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/django/utils/asyncio.py", line 26, in inner
	   return func(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/mysql/base.py", line 233, in get_new_connection
	   return Database.connect(**conn_params)
	 File "/usr/local/lib/python3.8/site-packages/MySQLdb/__init__.py", line 84, in Connect
	   return Connection(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/MySQLdb/connections.py", line 179, in __init__
	   super(Connection, self).__init__(*args, **kwargs2)
	MySQLdb._exceptions.OperationalError: (1040, 'Too many connections')
	
	The above exception was the direct cause of the following exception:
	
	Traceback (most recent call last):
	 File "/usr/local/lib/python3.8/site-packages/django/core/handlers/exception.py", line 34, in inner
	   response = get_response(request)
	 File "/usr/local/lib/python3.8/site-packages/django/core/handlers/base.py", line 115, in _get_response
	   response = self.process_exception_by_middleware(e, request)
	 File "/usr/local/lib/python3.8/site-packages/django/core/handlers/base.py", line 113, in _get_response
	   response = wrapped_callback(request, *callback_args, **callback_kwargs)
	 File "/usr/local/lib/python3.8/contextlib.py", line 74, in inner
	   with self._recreate_cm():
	 File "/usr/local/lib/python3.8/site-packages/django/db/transaction.py", line 175, in __enter__
	   if not connection.get_autocommit():
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/base/base.py", line 390, in get_autocommit
	   self.ensure_connection()
	 File "/usr/local/lib/python3.8/site-packages/django/utils/asyncio.py", line 26, in inner
	   return func(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/base/base.py", line 220, in ensure_connection
	   self.connect()
	 File "/usr/local/lib/python3.8/site-packages/django/db/utils.py", line 90, in __exit__
	   raise dj_exc_value.with_traceback(traceback) from exc_value
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/base/base.py", line 220, in ensure_connection
	   self.connect()
	 File "/usr/local/lib/python3.8/site-packages/django/utils/asyncio.py", line 26, in inner
	   return func(*args, **kwargs)"
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/base/base.py", line 197, in connect
	   self.connection = self.get_new_connection(conn_params)
	 File "/usr/local/lib/python3.8/site-packages/django_prometheus/db/common.py", line 44, in get_new_connection
	   return super(DatabaseWrapperMixin, self).get_new_connection(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/django/utils/asyncio.py", line 26, in inner
	   return func(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/django/db/backends/mysql/base.py", line 233, in get_new_connection
	   return Database.connect(**conn_params)
	 File "/usr/local/lib/python3.8/site-packages/MySQLdb/__init__.py", line 84, in Connect
	   return Connection(*args, **kwargs)
	 File "/usr/local/lib/python3.8/site-packages/MySQLdb/connections.py", line 179, in __init__
	   super(Connection, self).__init__(*args, **kwargs2)
	django.db.utils.OperationalError: (1040, 'Too many connections')`

	writeByLine(m, tracebackStr, time.Unix(100500, 0))
	msg := <-m.Messages
	assert.Equal(t, tracebackStr, msg.Content)
	assert.Equal(t, int64(100500), msg.Timestamp.Unix())

	tracebackStr = `2020-03-20 08:48:57,067 ERROR:__main__:Traceback (most recent call last):
	 File "<stdin>", line 2, in <module>
	 File "<stdin>", line 2, in do_something_that_might_error
	 File "<stdin>", line 2, in raise_error
	RuntimeError: something bad happened!`
	writeByLine(m, tracebackStr, time.Unix(0, 0))
	msg = <-m.Messages
	assert.Equal(t, tracebackStr, msg.Content)

	m.Add(LogEntry{Content: "E0504 07:38:36.184861       1 replica_set.go:450] starting worker #224", Level: LevelUnknown})
	m.Add(LogEntry{Content: "E0504 07:38:36.184861       1 replica_set.go:450] starting worker #225", Level: LevelUnknown})
	msg = <-m.Messages
	assert.Equal(t, "E0504 07:38:36.184861       1 replica_set.go:450] starting worker #224", msg.Content)
	msg = <-m.Messages
	assert.Equal(t, "E0504 07:38:36.184861       1 replica_set.go:450] starting worker #225", msg.Content)

	javaStackTraceStr := `ERROR [Messaging-EventLoop-3-1] 2023-10-04 14:27:35,249 2020-03-31 javax.servlet.ServletException: Something bad happened
		at com.example.myproject.OpenSessionInViewFilter.doFilter(OpenSessionInViewFilter.java:60)
		at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1157)
		at com.example.myproject.ExceptionHandlerFilter.doFilter(ExceptionHandlerFilter.java:28)
		at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1157)
		at com.example.myproject.OutputBufferFilter.doFilter(OutputBufferFilter.java:33)
		at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1157)
		at org.mortbay.jetty.servlet.ServletHandler.handle(ServletHandler.java:388)
		at org.mortbay.jetty.security.SecurityHandler.handle(SecurityHandler.java:216)
		at org.mortbay.jetty.servlet.SessionHandler.handle(SessionHandler.java:182)
		at org.mortbay.jetty.handler.ContextHandler.handle(ContextHandler.java:765)
		at org.mortbay.jetty.webapp.WebAppContext.handle(WebAppContext.java:418)
		at org.mortbay.jetty.handler.HandlerWrapper.handle(HandlerWrapper.java:152)
		at org.mortbay.jetty.Server.handle(Server.java:326)
		at org.mortbay.jetty.HttpConnection.handleRequest(HttpConnection.java:542)
		at org.mortbay.jetty.HttpConnection$RequestHandler.content(HttpConnection.java:943)
		at org.mortbay.jetty.HttpParser.parseNext(HttpParser.java:756)
		at org.mortbay.jetty.HttpParser.parseAvailable(HttpParser.java:218)
		at org.mortbay.jetty.HttpConnection.handle(HttpConnection.java:404)
		at org.mortbay.jetty.bio.SocketConnector$Connection.run(SocketConnector.java:228)
		at org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool.java:582)
	Caused by: com.example.myproject.MyProjectServletException
		at com.example.myproject.MyServlet.doPost(MyServlet.java:169)
		at javax.servlet.http.HttpServlet.service(HttpServlet.java:727)
		at javax.servlet.http.HttpServlet.service(HttpServlet.java:820)
		at org.mortbay.jetty.servlet.ServletHolder.handle(ServletHolder.java:511)
		at org.mortbay.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1166)
		at com.example.myproject.OpenSessionInViewFilter.doFilter(OpenSessionInViewFilter.java:30)
		... 27 more
	Caused by: org.hibernate.exception.ConstraintViolationException: could not insert: [com.example.myproject.MyEntity]
		at org.hibernate.exception.SQLStateConverter.convert(SQLStateConverter.java:96)
		at org.hibernate.exception.JDBCExceptionHelper.convert(JDBCExceptionHelper.java:66)
		at org.hibernate.id.insert.AbstractSelectingDelegate.performInsert(AbstractSelectingDelegate.java:64)
		at org.hibernate.persister.entity.AbstractEntityPersister.insert(AbstractEntityPersister.java:2329)
		at org.hibernate.persister.entity.AbstractEntityPersister.insert(AbstractEntityPersister.java:2822)
		at org.hibernate.action.EntityIdentityInsertAction.execute(EntityIdentityInsertAction.java:71)
		at org.hibernate.engine.ActionQueue.execute(ActionQueue.java:268)
		at org.hibernate.event.def.AbstractSaveEventListener.performSaveOrReplicate(AbstractSaveEventListener.java:321)
		at org.hibernate.event.def.AbstractSaveEventListener.performSave(AbstractSaveEventListener.java:204)
		at org.hibernate.event.def.AbstractSaveEventListener.saveWithGeneratedId(AbstractSaveEventListener.java:130)
		at org.hibernate.event.def.DefaultSaveOrUpdateEventListener.saveWithGeneratedOrRequestedId(DefaultSaveOrUpdateEventListener.java:210)
		at org.hibernate.event.def.DefaultSaveEventListener.saveWithGeneratedOrRequestedId(DefaultSaveEventListener.java:56)
		at org.hibernate.event.def.DefaultSaveOrUpdateEventListener.entityIsTransient(DefaultSaveOrUpdateEventListener.java:195)
		at org.hibernate.event.def.DefaultSaveEventListener.performSaveOrUpdate(DefaultSaveEventListener.java:50)
		at org.hibernate.event.def.DefaultSaveOrUpdateEventListener.onSaveOrUpdate(DefaultSaveOrUpdateEventListener.java:93)
		at org.hibernate.impl.SessionImpl.fireSave(SessionImpl.java:705)
		at org.hibernate.impl.SessionImpl.save(SessionImpl.java:693)
		at org.hibernate.impl.SessionImpl.save(SessionImpl.java:689)
		at sun.reflect.GeneratedMethodAccessor5.invoke(Unknown Source)
		at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:25)
		at java.lang.reflect.Method.invoke(Method.java:597)
		at org.hibernate.context.ThreadLocalSessionContext$TransactionProtectionWrapper.invoke(ThreadLocalSessionContext.java:344)
		at $Proxy19.save(Unknown Source)
		at com.example.myproject.MyEntityService.save(MyEntityService.java:59) <-- relevant call (see notes below)
		at com.example.myproject.MyServlet.doPost(MyServlet.java:164)
		... 32 more
	Caused by: java.sql.SQLException: Violation of unique constraint MY_ENTITY_UK_1: duplicate value(s) for column(s) MY_COLUMN in statement [...]
		at org.hsqldb.jdbc.Util.throwError(Unknown Source)
		at org.hsqldb.jdbc.jdbcPreparedStatement.executeUpdate(Unknown Source)
		at com.mchange.v2.c3p0.impl.NewProxyPreparedStatement.executeUpdate(NewProxyPreparedStatement.java:105)
		at org.hibernate.id.insert.AbstractSelectingDelegate.performInsert(AbstractSelectingDelegate.java:57)
		... 54 more`

	writeByLine(m, javaStackTraceStr, time.Unix(0, 0))
	msg = <-m.Messages
	assert.Equal(t, javaStackTraceStr, msg.Content)

	data := `Order response: {"statusCode":406,"body":{"timestamp":1648205755430,"status":406,"error":"Not Acceptable","exception":"works.weave.socks.orders.controllers.OrdersController$PaymentDeclinedException","message":"Payment declined: amount exceeds 100.00","path":"/orders"},"headers":{"x-application-context":"orders:80","content-type":"application/json;charset=UTF-8","transfer-encoding":"chunked","date":"Fri, 25 Mar 2022 10:55:55 GMT","connection":"close"},"request":{"uri":{"protocol":"http:","slashes":true,"auth":null,"host":"orders","port":80,"hostname":"orders","hash":null,"search":null,"query":null,"pathname":"/orders","path":"/orders","href":"http://orders/orders"},"method":"POST","headers":{"accept":"application/json","content-type":"application/json","content-length":232}}}
Order response: {"timestamp":1648205755430,"status":406,"error":"Not Acceptable","exception":"works.weave.socks.orders.controllers.OrdersController$PaymentDeclinedException","message":"Payment declined: amount exceeds 100.00","path":"/orders"}
`
	writeByLine(m, data, time.Unix(0, 0))
	msg = <-m.Messages
	assert.Equal(t, strings.Split(data, "\n")[0], msg.Content)
	msg = <-m.Messages
	assert.Equal(t, strings.Split(data, "\n")[1], msg.Content)
}
