PROJECT = elysium

DEPS = eper epocxy seestar vbisect
dep_epocxy  = git https://github.com/duomark/epocxy 0.9.8
dep_seestar = git https://github.com/tigertext/seestar 0.1.1
dep_vbisect = git https://github.com/jaynel/vbisect 0.1.2
V = 0

TEST_DEPS = proper test_commons
dep_proper       = git https://github.com/manopapad/proper master
dep_test_commons = git https://github.com/tigertext/test_commons master

PLATFORM_OPTS := `erlc -o ebin src/elysium_compile_utils.erl ; erl -noshell -pa ebin -s elysium_compile_utils platform_opts -s init stop`

ERLC_OPTS := +debug_info +"{cover_enabled, true}" ${PLATFORM_OPTS}

# Needed for testing
TEST_ERLC_OPTS := -I include $(ERLC_OPTS)
CT_OPTS := -cover test/elysium.coverspec
# CT_SUITES := elysium_basic

# DIALYZER_OPTS := -I include test/elysium -Werror_handling -Wrace_conditions -Wunmatched_returns
DIALYZER_OPTS ?= -r deps/epocxy/src ${PLATFORM_OPTS} -Werror_handling -Wrace_conditions -Wunmatched_returns

## EDOC_DIRS := ["src", "examples"]
## EDOC_OPTS := {preprocess, true}, {source_path, ${EDOC_DIRS}}, nopackages, {subpackages, true}

ERL_PATH := -smp enable -pa ebin deps/*/ebin
SERVER := erl $(ERL_PATH)

include erlang.mk

run: all
	$(SERVER) -mode embedded -s elysium

dev: all
	$(SERVER) -pa test

images: doc
	mkdir -p doc/images
	dot -Tpng doc/states.dot -o doc/images/states.png

clean::
	rm -rf logs
