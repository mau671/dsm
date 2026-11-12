# ==============================================================================
# Makefile — DSM Prototipo 1
# ==============================================================================

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

.PHONY: all release debug asan tsan clean test test-unit test-int

all: release

# Macro para instanciar reglas por perfil
define build_profile
$$(BUILD)/$(1):
	mkdir -p $$@

$$(BUILD)/$(1)/%.o: src/%.c | $$(BUILD)/$(1)
	$$(CC) $$(CFLAGS_COMMON) $$(CFLAGS_$(2)) -MMD -MP -c $$< -o $$@

$$(BUILD)/$(1)/libdsm.a: $$(patsubst src/%.c,$$(BUILD)/$(1)/%.o,$$(LIB_SRCS)) | $$(BUILD)/$(1)
	$$(AR) rcs $$@ $$^

$$(BUILD)/$(1)/%: apps/%.c $$(BUILD)/$(1)/libdsm.a | $$(BUILD)/$(1)
	$$(CC) $$(CFLAGS_COMMON) $$(CFLAGS_$(2)) $$< -L$$(BUILD)/$(1) -ldsm \
	      $$(LDFLAGS_COMMON) $$(LDFLAGS_$(2)) -o $$@

$$(BUILD)/$(1)/dsmctl: tools/dsmctl.c $$(BUILD)/$(1)/libdsm.a | $$(BUILD)/$(1)
	$$(CC) $$(CFLAGS_COMMON) $$(CFLAGS_$(2)) $$< -L$$(BUILD)/$(1) -ldsm \
	      $$(LDFLAGS_COMMON) $$(LDFLAGS_$(2)) -o $$@
endef

profile_release := OPT
profile_debug   := DEBUG
profile_asan    := ASAN
profile_tsan    := TSAN

$(foreach p,$(PROFILES),$(eval $(call build_profile,$(p),$(profile_$(p)))))

release: $(BUILD)/release/libdsm.a $(BUILD)/release/dsmd $(BUILD)/release/dsmctl
debug:   $(BUILD)/debug/libdsm.a   $(BUILD)/debug/dsmd   $(BUILD)/debug/dsmctl \
         $(BUILD)/debug/test_basico
asan:    $(BUILD)/asan/libdsm.a    $(BUILD)/asan/dsmd    $(BUILD)/asan/dsmctl
tsan:    $(BUILD)/tsan/libdsm.a    $(BUILD)/tsan/dsmd    $(BUILD)/tsan/dsmctl

# --- Tests unitarios (enlazan con asan para chequeo de memoria) ---
$(BUILD)/unit:
	mkdir -p $@

TEST_SRCS := $(wildcard tests/unit/test_*.c)
TEST_BINS := $(patsubst tests/unit/%.c,$(BUILD)/unit/%,$(TEST_SRCS))

$(BUILD)/unit/%: tests/unit/%.c $(BUILD)/asan/libdsm.a | $(BUILD)/unit
	$(CC) $(CFLAGS_COMMON) $(CFLAGS_ASAN) -Itests/harness $< -L$(BUILD)/asan -ldsm \
	      $(LDFLAGS_COMMON) $(LDFLAGS_ASAN) -o $@

test-unit: asan $(TEST_BINS)
	@echo "=== Ejecutando pruebas unitarias ==="
	@set -e; for t in $(TEST_BINS); do echo ">> Ejecutando $$t"; ./$$t; echo ""; done
	@echo "Todas las pruebas unitarias pasaron exitosamente."

# --- Integracion de dos nodos con la API publica ---
$(BUILD)/integration:
	mkdir -p $@

$(BUILD)/integration/test_two_nodes: tests/integration/test_two_nodes.c | $(BUILD)/integration
	$(CC) $(CFLAGS_COMMON) $< $(LDFLAGS_COMMON) -o $@

test-int: debug $(BUILD)/integration/test_two_nodes
	$(BUILD)/integration/test_two_nodes

# --- Red del compañero: pruebas aisladas de la capa de red ---
test: test-unit

clean:
	rm -rf $(BUILD)

-include $(wildcard $(BUILD)/*/*.d)
