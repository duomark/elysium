%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, Tigertext
%%% @author George Ye <gye@tigertext.com> [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Serializer to force a single writer to elysium_connection_queue
%%%   ets_buffer so that concurrency errors can be avoided which caused
%%%   readers to skip over late arriving connection checkins.
%%% @since 0.1.6g
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_session_enqueuer).

-behaviour(gen_server).

%% API exports.
-export([start_link/1, checkin_session/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-include("elysium_types.hrl").

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(config_type()) -> {ok, pid()}.
start_link(Config) ->
    true = elysium_config:is_valid_config(Config),
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Config}, []).

checkin_session(Session_Queue, Session_Data) ->
    gen_server:call(?MODULE, {checkin, Session_Queue, Session_Data}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

%% @private
init({_Config}) ->
    {ok, []}.

%% @private
terminate(_Reason, _St) ->
    ok.

%% @private

handle_call({checkin, Session_Queue, Session_Data} =_Request, _From, St) ->
    Available  = ets_buffer:write_dedicated(Session_Queue, Session_Data),
    {reply, Available, St};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

%% @private

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

%% @private
handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.


%% @private
code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
