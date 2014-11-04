%%% Copyright 2014 Aleksey Yeschenko
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.

%%% Forked as a modified copy of seestar_session to remove multiplexed
%%% session channels and to allow gen_server and gen_tcp timeouts.

-module(elysium_channel).

-behaviour(gen_server).

-include_lib("seestar/src/seestar_messages.hrl").
-include_lib("seestar/include/builtin_types.hrl").
-include_lib("seestar/include/buffer.hrl").

%% API exports.
-export([start_link/2, start_link/3, start_link/4, stop/1]).
-export([perform/3, perform_async/3]).
-export([prepare/2, execute/5, execute_async/5]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-type credentials() :: [{string() | binary(), string() | binary()}].
-type events() :: [topology_change | status_change | schema_change].
-type client_option() :: {keyspace, string() | binary()}
                       | {credentials, credentials()}
                       | {events, events()}
                       | {send_timeout, non_neg_integer()}.

-type connect_option() :: gen_tcp:connect_option() | {connect_timeout, timeout()}.

-type 'query'() :: binary() | string().
-type query_id() :: binary().

-define(b2l(Term), case is_binary(Term) of true -> binary_to_list(Term); false -> Term end).
-define(l2b(Term), case is_list(Term) of true -> list_to_binary(Term); false -> Term end).

-record(req,
        {op   :: seestar_frame:opcode(),
         body :: binary(),
         from :: {pid(), reference()},
         sync = true :: boolean()
        }).

-record(st,
        {host   :: inet:hostname(),
         sock   :: inet:socket(),
         buffer :: seestar_buffer:buffer(),
         from   :: {pid(), reference()},
         sync   :: boolean()
        }).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

%% @equiv start_link(Host, Post, [])
-spec start_link(inet:hostname(), inet:port_number()) ->
    any().
start_link(Host, Port) ->
    start_link(Host, Port, []).

%% @equiv start_link(Host, Post, ClientOptions, [])
-spec start_link(inet:hostname(), inet:port_number(), [client_option()]) ->
    any().
start_link(Host, Port, ClientOptions) ->
    start_link(Host, Port, ClientOptions, []).

-spec start_link(inet:hostname(), inet:port_number(), [client_option()], [connect_option()]) ->
    any().
start_link(Host, Port, ClientOptions, ConnectOptions) ->
     case gen_server:start_link(?MODULE, [Host, Port, ConnectOptions], []) of
        {ok, Pid} ->
            case setup(Pid, ClientOptions) of
                ok    -> {ok, Pid};
                Error -> stop(Pid), Error
            end;
        Error ->
            Error
    end.

setup(Pid, Options) ->
    case authenticate(Pid, Options) of
        false ->
            {error, invalid_credentials};
        true ->
            case set_keyspace(Pid, Options) of
                false ->
                    {error, invalid_keyspace};
                true ->
                    case subscribe(Pid, Options) of
                        false -> {error, invalid_events};
                        true  -> ok
                    end
            end
    end.

authenticate(Pid, Options) ->
    Credentials = proplists:get_value(credentials, Options),
    case request(Pid, #startup{}, true) of
        #ready{} ->
            true;
        #authenticate{} when Credentials =:= undefined ->
            false;
        #authenticate{} ->
            KVPairs = [ {?l2b(K), ?l2b(V)} || {K, V} <- Credentials ],
            case request(Pid, #credentials{credentials = KVPairs}, true) of
                #ready{} -> true;
                #error{} -> false
            end
    end.

set_keyspace(Pid, Options) ->
    case proplists:get_value(keyspace, Options) of
        undefined ->
            true;
        Keyspace ->
            case perform(Pid, "USE " ++ ?b2l(Keyspace), one) of
                {ok, _Result}    -> true;
                {error, _Reason} -> false
            end
    end.

subscribe(Pid, Options) ->
    case proplists:get_value(events, Options, []) of
        [] ->
            true;
        Events ->
            case request(Pid, #register{event_types = Events}, true) of
                #ready{} -> true;
                #error{} -> false
            end
    end.

%% @doc Stop the client.
%% Closes the socket and terminates the process normally.
-spec stop(pid()) -> ok.
stop(Client) ->
    gen_server:cast(Client, stop).

%% @doc Synchoronously perform a CQL query using the specified consistency level.
%% Returns a result of an appropriate type (void, rows, set_keyspace, schema_change).
%% Use {@link seestar_result} module functions to work with the result.
-spec perform(pid(), 'query'(), seestar:consistency()) ->
    {ok, Result :: seestar_result:result()} | {error, Error :: seestar_error:error()} | busy.
perform(Client, Query, Consistency) ->
    case request(Client, #'query'{'query' = ?l2b(Query), consistency = Consistency}, true) of
        #result{result = Result} ->
            {ok, Result};
        #error{} = Error ->
            {error, Error};
        busy ->
            busy
    end.

%% TODO doc
%% @doc Asynchronously perform a CQL query using the specified consistency level.
-spec perform_async(pid(), 'query'(), seestar:consistency()) -> ok | busy.
perform_async(Client, Query, Consistency) ->
    Req = #'query'{'query' = ?l2b(Query), consistency = Consistency},
    request(Client, Req, false).

%% @doc Prepare a query for later execution. The response will contain the prepared
%% query id and column metadata for all the variables (if any).
%% @see execute/3.
%% @see execute/4.
-spec prepare(pid(), 'query'()) ->
    {ok, Result :: seestar_result:prepared_result()} | {error, Error :: seestar_error:error()} | busy.
prepare(Client, Query) ->
    case request(Client, #prepare{'query' = ?l2b(Query)}, true) of
        #result{result = Result} ->
            {ok, Result};
        #error{} = Error ->
            {error, Error};
        busy ->
            busy
    end.

%% @doc Synchronously execute a prepared query using the specified consistency level.
%% Use {@link seestar_result} module functions to work with the result.
%% @see prepare/2.
%% @see perform/3.
-spec execute(pid(), query_id(),
              [seestar_cqltypes:type()], [seestar_cqltypes:value()], seestar:consistency()) ->
        {ok, Result :: seestar_result:result()} | {error, Error :: seestar_error:error()} | busy.
execute(Client, QueryID, Types, Values, Consistency) ->
    Req = #execute{id = QueryID, types = Types, values = Values, consistency = Consistency},
    case request(Client, Req, true) of
        #result{result = Result} ->
            {ok, Result};
        #error{} = Error ->
            {error, Error};
        busy ->
            busy
    end.

-spec execute_async(pid(), query_id(),
                    [seestar_cqltypes:type()], [seestar_cqltypes:value()], seestar:consistency()) ->
        {ok, Result :: seestar_result:result()} | {error, Error :: seestar_error:error()} | busy.
execute_async(Client, QueryID, Types, Values, Consistency) ->
    Req = #execute{id = QueryID, types = Types, values = Values, consistency = Consistency},
    request(Client, Req, false).

request(Client, Request, Sync) ->
    {ReqOp, ReqBody} = seestar_messages:encode(Request),
    case gen_server:call(Client, {request, ReqOp, ReqBody, Sync}, 30000) of
        {RespOp, RespBody} ->
            seestar_messages:decode(RespOp, RespBody);
        Ref ->
            Ref
    end.

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

%% @private
init([Host, Port, ConnectOptions]) ->
    Timeout = proplists:get_value(connect_timeout, ConnectOptions, infinity),
    SockOpts = proplists:delete(connect_timeout, ConnectOptions),
    case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
        {ok, Sock} ->
            ok = inet:setopts(Sock, [binary, {packet, 0}, {active, true}]),
            {ok, #st{host = Host, sock = Sock, buffer = seestar_buffer:new()}};
        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.

%% @private
terminate(_Reason, _St) ->
    ok.

%% @private
handle_call({request, _Op, _Body, _Sync}, {_Pid, _Ref} = _From, #st{from=Current_Request} = St)
  when Current_Request =/= undefined ->
    {reply, busy, St};
handle_call({request, Op, Body, Sync}, {_Pid, Ref} = From, St) ->
    Sync orelse gen_server:reply(From, Ref),
    case send_request(#req{op = Op, body = Body, from = From, sync = Sync}, St) of
        {ok, St1}       -> {noreply, St1#st{from = From, sync = Sync}};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end;

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

send_request(#req{op = Op, body = Body}, St) ->
    ID = 1,   % Only one request at a time, so Id shouldn't matter.
    Frame = seestar_frame:new(ID, [], Op, Body),
    case gen_tcp:send(St#st.sock, seestar_frame:encode(Frame)) of
        ok    -> {ok, St};
        Error -> Error
    end.

%% @private
handle_cast(stop, St) ->
    gen_tcp:close(St#st.sock),
    {stop, normal, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

%% @private
handle_info({tcp, Sock, Data}, #st{sock = Sock} = St) ->
    {Frames, Buffer} = seestar_buffer:decode(St#st.buffer, Data),
    case Buffer#buffer.pending_size of
        0          -> St1 = process_frames(Frames, St#st{buffer = Buffer}),
                      {noreply, St1#st{from = undefined, sync = undefined}};
        _Remaining -> St1 = process_frames(Frames, St#st{buffer = Buffer}),
                      {noreply, St1}
    end;

handle_info({tcp_closed, Sock}, #st{sock = Sock} = St) ->
    {stop, socket_closed, St};

handle_info({tcp_error, Sock, Reason}, #st{sock = Sock} = St) ->
    {stop, {socket_error, Reason}, St};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

process_frames(              [], St) -> St;
process_frames([Frame | Frames], St) ->
    process_frames(Frames,
                   case seestar_frame:id(Frame) of
                       -1 -> St;
                       _  -> handle_response(Frame, St)
                   end).

handle_response(Frame, #st{from=From, sync=Sync} = St) ->
    %% ID = seestar_frame:id(Frame),
    Op = seestar_frame:opcode(Frame),
    Body = seestar_frame:body(Frame),
    case Sync of
        true  -> gen_server:reply(From, {Op, Body});
        false -> reply_async(From, Op, Body)
    end,
    St.

reply_async({Pid, Ref}, Op, Body) ->
    F = fun() ->
            case seestar_messages:decode(Op, Body) of
                #result{result = Result} ->
                    {ok, Result};
                #error{} = Error ->
                    {error, Error}
            end
        end,
    Pid ! {seestar_response, Ref, F}.

%% @private
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
