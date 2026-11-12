CC      ?= cc
AR      ?= ar
BUILD   ?= build

CFLAGS_COMMON := -std=c11 -Wall -Wextra -Wpedantic -Wshadow \
                 -Wstrict-prototypes -Wmissing-prototypes -Wcast-align \
                 -Wpointer-arith -Wvla -Wformat=2 -Werror -D_GNU_SOURCE \
                 -DMAX_NODES=256 -pthread -Iinclude \
                 -ftls-model=initial-exec

LDFLAGS_COMMON := -pthread -Wl,-z,now -Wl,-z,relro

# --- Perfiles de compilación ---
CFLAGS_OPT   := -O2 -DNDEBUG
CFLAGS_DEBUG := -g3 -O0 -fno-omit-frame-pointer -fno-common
CFLAGS_ASAN  := $(CFLAGS_DEBUG) -fsanitize=address,undefined \
                -fno-sanitize-recover=undefined -DDSM_TEST_HOOKS=1
CFLAGS_TSAN  := $(CFLAGS_DEBUG) -fsanitize=thread -DDSM_TSAN=1

LDFLAGS_OPT   :=
LDFLAGS_DEBUG :=
LDFLAGS_ASAN  := -fsanitize=address,undefined
LDFLAGS_TSAN  := -fsanitize=thread

PROFILES := release debug asan tsan

LIB_SRCS := $(wildcard src/*.c)

.PHONY: all release debug asan tsan clean test test-unit test-e2e

all: release

# Cada perfil deja los objetos en build/obj/<perfil>, la biblioteca en
# build/lib/<perfil> y los ejecutables en build/bin/<perfil>.
define build_profile
$$(BUILD)/obj/$(1):
	mkdir -p $$@

$$(BUILD)/lib/$(1):
	mkdir -p $$@

$$(BUILD)/bin/$(1):
	mkdir -p $$@

$$(BUILD)/obj/$(1)/%.o: src/%.c | $$(BUILD)/obj/$(1)
	$$(CC) $$(CFLAGS_COMMON) $$(CFLAGS_$(2)) -MMD -MP -c $$< -o $$@

$$(BUILD)/lib/$(1)/libdsm.a: $$(patsubst src/%.c,$$(BUILD)/obj/$(1)/%.o,$$(LIB_SRCS)) | $$(BUILD)/lib/$(1)
	$$(AR) rcs $$@ $$^

$$(BUILD)/bin/$(1)/%: apps/%.c $$(BUILD)/lib/$(1)/libdsm.a | $$(BUILD)/bin/$(1)
	$$(CC) $$(CFLAGS_COMMON) $$(CFLAGS_$(2)) $$< -L$$(BUILD)/lib/$(1) -ldsm \
	      $$(LDFLAGS_COMMON) $$(LDFLAGS_$(2)) -o $$@

$$(BUILD)/bin/$(1)/dsmctl: tools/dsmctl.c $$(BUILD)/lib/$(1)/libdsm.a | $$(BUILD)/bin/$(1)
	$$(CC) $$(CFLAGS_COMMON) $$(CFLAGS_$(2)) $$< -L$$(BUILD)/lib/$(1) -ldsm \
	      $$(LDFLAGS_COMMON) $$(LDFLAGS_$(2)) -o $$@

$$(BUILD)/bin/$(1)/video_pack: tools/video_pack.c | $$(BUILD)/bin/$(1)
	$$(CC) $$(CFLAGS_COMMON) $$(CFLAGS_$(2)) $$< \
	      $$(LDFLAGS_COMMON) $$(LDFLAGS_$(2)) -o $$@
endef

profile_release := OPT
profile_debug   := DEBUG
profile_asan    := ASAN
profile_tsan    := TSAN

$(foreach p,$(PROFILES),$(eval $(call build_profile,$(p),$(profile_$(p)))))

release: $(BUILD)/lib/release/libdsm.a \
         $(BUILD)/bin/release/dsm $(BUILD)/bin/release/dsmctl \
         $(BUILD)/bin/release/filtro_video $(BUILD)/bin/release/juego_vida \
         $(BUILD)/bin/release/video_pack
debug:   $(BUILD)/lib/debug/libdsm.a \
         $(BUILD)/bin/debug/dsm $(BUILD)/bin/debug/dsmctl \
         $(BUILD)/bin/debug/test_basico $(BUILD)/bin/debug/filtro_video \
         $(BUILD)/bin/debug/juego_vida $(BUILD)/bin/debug/video_pack
asan:    $(BUILD)/lib/asan/libdsm.a \
         $(BUILD)/bin/asan/dsm $(BUILD)/bin/asan/dsmctl
tsan:    $(BUILD)/lib/tsan/libdsm.a \
         $(BUILD)/bin/tsan/dsm $(BUILD)/bin/tsan/dsmctl

# --- Pruebas unitarias (enlazan con la biblioteca instrumentada de asan) ---
$(BUILD)/bin/unit:
	mkdir -p $@

TEST_SRCS := $(wildcard tests/unit/test_*.c)
TEST_BINS := $(patsubst tests/unit/%.c,$(BUILD)/bin/unit/%,$(TEST_SRCS))

$(BUILD)/bin/unit/%: tests/unit/%.c $(BUILD)/lib/asan/libdsm.a | $(BUILD)/bin/unit
	$(CC) $(CFLAGS_COMMON) $(CFLAGS_ASAN) -Itests/harness $< -L$(BUILD)/lib/asan -ldsm \
	      $(LDFLAGS_COMMON) $(LDFLAGS_ASAN) -o $@

test-unit: asan $(TEST_BINS)
	@echo "=== Ejecutando pruebas unitarias ==="
	@set -e; for t in $(TEST_BINS); do echo ">> Ejecutando $$t"; ./$$t; echo ""; done
	@echo "Todas las pruebas unitarias pasaron exitosamente."

# --- Prueba de extremo a extremo con dos nodos reales ---
$(BUILD)/bin/e2e:
	mkdir -p $@

$(BUILD)/bin/e2e/test_two_nodes: tests/e2e/test_two_nodes.c | $(BUILD)/bin/e2e
	$(CC) $(CFLAGS_COMMON) $< $(LDFLAGS_COMMON) -o $@

test-e2e: debug $(BUILD)/bin/e2e/test_two_nodes
	$(BUILD)/bin/e2e/test_two_nodes

test: test-unit

clean:
	rm -rf $(BUILD)

-include $(wildcard $(BUILD)/obj/*/*.d)
