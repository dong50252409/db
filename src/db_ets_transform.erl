%%%-------------------------------------------------------------------
%%% @author gz1417
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% 通过parse_transform的方式将调用ets模块的字句替换为db_ets模块
%%%
%%% 以下函数调用将会被替换
%%% delete/1, delete/2, delete_all_objects/1, delete_object/2, select_delete/2, match_delete/2, take/2,
%%% insert/2, insert_new/2,
%%% select_replace/2, update_counter/3, update_counter/4, update_element/3
%%%
%%% @end
%%% Created : 29. 9月 2021 17:08
%%%-------------------------------------------------------------------
-module(db_ets_transform).

-export_type([reg_info/0]).

-type reg_info() ::
{ets:tab(), db_mysql:db_pool(), module(), [db_mysql:condition()], [db_ets:option()]}|
{ets:tab(), db_mysql:db_pool(), module(), [db_ets:option()]}.

-callback(reg_list() -> [reg_info()]).

%% API
-export([parse_transform/2]).

%%-export([test/0]).
%%
%%test() ->
%%    {ok, String} = file:read_file("ast.config"),
%%    {ok, Tokens, _} = erl_scan:string(unicode:characters_to_list(<<String/binary, ".">>)),
%%    {ok, AST} = erl_parse:parse_term(Tokens),
%%    Options = [{db_ets_transform, db_ets_reg_list}],
%%    parse_transform(AST, Options).

-define(MODIFY_FUNC_LIST, [
    delete, delete, delete_all_objects, delete_object, select_delete, match_delete, take,
    insert, insert_new,
    select_replace, update_counter, update_counter, update_element
]).

parse_transform(AST, Options) ->
%%    file:write_file("ast.config", io_lib:format("~p", [AST])),
    Mod = proplists:get_value(db_ets_callback, Options),
    case load_file(Mod) of
        true ->
            RegList = Mod:reg_list(),
            Verbose = verbose(AST, Options),
            Fun = fun(Form) -> walk_call(Form, RegList, Verbose) end,
            NewAST = map(Fun, AST),
%%            file:write_file("new_ast.config", io_lib:format("~p", [NewAST])),
            NewAST;
        false ->
            AST
    end.

load_file(Mod) ->
    case code:is_loaded(Mod) of
        false ->
            case code:load_file(Mod) of
                {module, Mod} ->
                    erlang:function_exported(Mod, reg_list, 0);
                _Err ->
                    false
            end;
        _ ->
            erlang:function_exported(Mod, reg_list, 0)
    end.

map(Fun, AST) ->
    [map_category(Form, Fun) || Form <- AST].

map_category({attribute, Line, record, {E1, E2}}, Fun) ->
    Fun({attribute, Line, record, {map_category(E1, Fun), map_category(E2, Fun)}});
map_category({attribute, Line, spec, {E1, E2}}, Fun) ->
    Fun({attribute, Line, spec, {map_category(E1, Fun), map_category(E2, Fun)}});
map_category({attribute, Line, callback, {E1, E2}}, Fun) ->
    Fun({attribute, Line, callback, {map_category(E1, Fun), map_category(E2, Fun)}});
map_category({attribute, Line, type, {E1, E2, E3}}, Fun) ->
    Fun({attribute, Line, type, {map_category(E1, Fun), map_category(E2, Fun), map_category(E3, Fun)}});
map_category({attribute, Line, opaque, {E1, E2, E3}}, Fun) ->
    Fun({attribute, Line, opaque, {map_category(E1, Fun), map_category(E2, Fun), map_category(E3, Fun)}});
map_category({attribute, Line, Attr, Val}, Fun) ->
    Fun({attribute, Line, map_category(Attr, Fun), map_category(Val, Fun)});
map_category({function, F, A}, Fun) ->
    Fun({function, map_category(F, Fun), map_category(A, Fun)});
map_category({function, M, F, A}, Fun) ->
    Fun({function, map_category(M, Fun), map_category(F, Fun), map_category(A, Fun)});
map_category({clauses, Cs}, Fun) ->
    Fun({clauses, map_category(Cs, Fun)});
map_category({typed_record_field, Field, Type}, Fun) ->
    Fun({typed_record_field, map_category(Field, Fun), map_category(Type, Fun)});
map_category({_Tag, _} = Form, Fun) ->
    Fun(Form);
map_category({Tag, Line, E1}, Fun) ->
    Fun({Tag, Line, map_category(E1, Fun)});
map_category({Tag, Line, E1, E2}, Fun) ->
    Fun({Tag, Line, map_category(E1, Fun), map_category(E2, Fun)});
map_category({bin_element, Line, E1, E2, TSL}, Fun) ->
    Fun({bin_element, Line, map_category(E1, Fun), map_category(E2, Fun), TSL});
map_category({Tag, Line, E1, E2, E3}, Fun) ->
    Fun({Tag, Line, map_category(E1, Fun), map_category(E2, Fun), map_category(E3, Fun)});
map_category({Tag, Line, E1, E2, E3, E4}, Fun) ->
    Fun({Tag, Line, map_category(E1, Fun), map_category(E2, Fun), map_category(E3, Fun), map_category(E4, Fun)});
map_category([H | T], Fun) ->
    [map_category(H, Fun) | map_category(T, Fun)];
map_category([], _Fun) -> [];
map_category(E, _Fun) when not is_tuple(E), not is_list(E) -> E.

walk_call({call, Line1, {remote, Line2, {atom, Line3, ets}, {atom, Line4, new}}, [{atom, _, Tab} = A1, Cons] = Args} = Form, RegList, Verbose) ->
    case lists:keyfind(Tab, 1, RegList) of
        false ->
            Form;
        {Tab, DBPool, ModName, Options} ->
            AppendArgs = [{db_pool, DBPool}, {mod_name, ModName}, {options, Options}],
            ConsForm = add_cons_list(AppendArgs, Cons),
            Verbose(Line1, new, erlang:length(Args), AppendArgs),
            {call, Line1, {remote, Line2, {atom, Line3, db_ets}, {atom, Line4, new}}, [A1, ConsForm]};
        {Tab, DBPool, ModName, Conditions, Options} ->
            AppendArgs = [{db_pool, DBPool}, {mod_name, ModName}, {conditions, Conditions}, {options, Options}],
            ConsForm = add_cons_list(AppendArgs, Cons),
            Verbose(Line1, new, erlang:length(Args), AppendArgs),
            {call, Line1, {remote, Line2, {atom, Line3, db_ets}, {atom, Line4, new}}, [A1, ConsForm]}
    end;
walk_call({call, Line1, {remote, Line2, {atom, Line3, ets}, {atom, Line4, Func}}, [{atom, _, Tab} | _] = Args} = Form, RegList, Verbose) ->
    case lists:keymember(Tab, 1, RegList) andalso lists:member(Func, ?MODIFY_FUNC_LIST) of
        true ->
            Verbose(Line1, Func, erlang:length(Args), []),
            {call, Line1, {remote, Line2, {atom, Line3, db_ets}, {atom, Line4, Func}}, Args};
        false ->
            Form
    end;
walk_call(Form, _RegList, _Verbose) ->
    Form.

add_cons_list([H | T], Cons) ->
    add_cons(erl_parse:abstract(H), add_cons_list(T, Cons));
add_cons_list([], Cons) ->
    Cons.

add_cons(AddForm, {cons, Line, _, _} = Cons) ->
    {cons, Line, AddForm, Cons}.

verbose(AST, Options) ->
    case proplists:is_defined(db_ets_verbose, Options) of
        true ->
            {attribute, _Line, module, Module} = lists:keyfind(module, 3, AST),
            fun(Line, Fun, Arity, AppendArgs) ->
                io:format("Module:~w Line:~w ets:~w/~w => db_ets:~w/~w AppendArgs:~w~n", [Module, Line, Fun, Arity, Fun, Arity, AppendArgs])
            end;
        false ->
            fun() -> ok end
    end.