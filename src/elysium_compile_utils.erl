%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014-2015, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014-2015 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Functions that Makefile can invoke from erl -s which returns things
%%%   like the OTP Release.
%%%
%%% @since 0.1.7
%%% @end
%%%------------------------------------------------------------------------------
-module(elysium_compile_utils).

-export([platform_opts/0]).

platform_opts() ->
    case erlang:system_info(otp_release) of
        "R" ++ _Release_Below_17 -> io:format("-Donly_builtin_types");
        "17"   -> io:format("-Dexperimental_maps");
        "18"   -> io:format("-Doperational_maps");
        _Other -> io:format("")
    end.
