PROJECT = erl_rethink
DEPS = jsx

dep_jsv = git https://github.com/talentdeficit/jsx master

include erlang.mk

run: app
	erl -pa ebin deps/*/ebin -name erl_rethink@localhost -s erl_rethink
