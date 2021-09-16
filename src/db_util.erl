%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% 一些工具
%%% @end
%%% Created : 25. 8月 2021 17:25
%%%-------------------------------------------------------------------
-module(db_util).

%% API
-export([term_to_string/1, string_to_term/1, string_to_term/2, blob_to_term/1, blob_to_term/2, term_to_blob/1, json_to_term/1, term_to_json/1]).

%%------------------------------------------------------------------------------
%% @doc
%% 将Erlang项式转为String
%% @end
%%------------------------------------------------------------------------------
-spec term_to_string(term()) -> binary().
term_to_string(Term) ->
    list_to_binary(io_lib:format("~w", [Term])).

%%------------------------------------------------------------------------------
%% @doc
%% 将String转为Erlang项式
%% @end
%%------------------------------------------------------------------------------
-spec string_to_term(binary()|list()) -> term().
string_to_term(String) when is_binary(String) ->
    string_to_term(binary_to_list(String));
string_to_term(String) when is_list(String) ->
    {ok, Tokens, _} = erl_scan:string(String ++ "."),
    {ok, Value} = erl_parse:parse_term(Tokens),
    Value.

%%------------------------------------------------------------------------------
%% @doc
%% 将String转为Erlang项式，如果为空或转换失败，则返回默认值
%% @end
%%------------------------------------------------------------------------------
-spec string_to_term(binary()|list(), term()) -> term().
string_to_term(<<>>, DefaultValue) ->
    DefaultValue;
string_to_term([], DefaultValue) ->
    DefaultValue;
string_to_term(String, DefaultValue) when is_binary(String) ->
    string_to_term(binary_to_list(String), DefaultValue);
string_to_term(String, DefaultValue) when is_list(String) ->
    try
        string_to_term(String)
    catch
        _Err:_Reason ->
            DefaultValue
    end.


%%------------------------------------------------------------------------------
%% @doc
%% 将Blob二进制数据转为Erlang项式
%% @end
%%------------------------------------------------------------------------------
-spec blob_to_term(Blob :: binary()) -> term().
blob_to_term(Blob) when is_binary(Blob) ->
    binary_to_term(Blob).

%%------------------------------------------------------------------------------
%% @doc
%% 将Blob二进制数据转为Erlang项式，如果为空或转换失败则返回默认值
%% @end
%%------------------------------------------------------------------------------
-spec blob_to_term(Blob :: binary(), DefaultValue :: term()) -> term().
blob_to_term(<<>>, DefaultValue) ->
    DefaultValue;
blob_to_term(Blob, DefaultValue) when is_binary(Blob) ->
    try
        binary_to_term(Blob)
    catch
        _Err:_Reason ->
            DefaultValue
    end.

%%------------------------------------------------------------------------------
%% @doc
%% 将Erlang项式转为Blob二进制数据
%% @end
%%------------------------------------------------------------------------------
-spec term_to_blob(Term :: term()) -> binary().
term_to_blob(Term) ->
    term_to_binary(Term).


%%------------------------------------------------------------------------------
%% @doc
%% 将JSON转为Erlang项式
%% @end
%%------------------------------------------------------------------------------
-spec json_to_term(JSON :: binary()) -> term().
json_to_term(JSON) ->
    jsx:decode(JSON).

%%------------------------------------------------------------------------------
%% @doc
%% 将Erlang项式转为JSON
%% @end
%%------------------------------------------------------------------------------
-spec term_to_json(Term :: map()|tuple()|list()) -> binary().
term_to_json(Term) ->
    jsx:encode(Term).

