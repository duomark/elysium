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
