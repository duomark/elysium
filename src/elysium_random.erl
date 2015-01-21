%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Random utility functions used for decay and restart delays.
%%%
%%% @since 0.1.3
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_random).
-author('jay@duomark.com').

%% External API
-export([random_int_up_to/1]).

random_int_up_to(Max) ->
    _ = maybe_seed(),
    random:uniform(Max).

maybe_seed() ->
    case get(random_seed) of
        undefined -> random:seed(os:timestamp());
        {X, X, X} -> random:seed(os:timestamp());
        _         -> ok
    end.
