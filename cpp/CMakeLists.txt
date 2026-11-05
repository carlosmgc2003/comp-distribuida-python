cmake_minimum_required(VERSION 3.25)
project(comp_distribuida_cpp
        VERSION 2.0.0
        DESCRIPTION "Núcleo de Jacobi por bloques para el TP de computación distribuida"
        LANGUAGES CXX)

# --- Opciones -----------------------------------------------------------------
# Nota: NO se fija CMAKE_BUILD_TYPE aquí (el original forzaba Release, pisando
# lo que pidiera el usuario). Usar los presets: `cmake --preset release`.
option(JACOBI_BUILD_TESTS "Compilar los tests (Catch2)" ${PROJECT_IS_TOP_LEVEL})
option(JACOBI_BUILD_CLI "Compilar la CLI jacobi_cli" ${PROJECT_IS_TOP_LEVEL})
option(JACOBI_BUILD_PYTHON "Compilar el módulo de Python (nanobind)" OFF)
option(JACOBI_SANITIZERS "Compilar con AddressSanitizer + UBSan" OFF)
option(JACOBI_WARNINGS_AS_ERRORS "Tratar warnings como errores" OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)  # necesario para enlazar en el módulo de Python

# --- Flags comunes como target de interfaz --------------------------------------
# (en el original se usaba CMAKE_CXX_FLAGS_ALL, que no es una variable de CMake:
#  -Wall -Wextra nunca se aplicaban)
add_library(jacobi_flags INTERFACE)
target_compile_features(jacobi_flags INTERFACE cxx_std_23)
if(MSVC)
  target_compile_options(jacobi_flags INTERFACE /W4 /permissive- $<$<BOOL:${JACOBI_WARNINGS_AS_ERRORS}>:/WX>)
else()
  target_compile_options(jacobi_flags INTERFACE
    -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
    -Wnon-virtual-dtor -Wold-style-cast -Wnull-dereference -Wdouble-promotion
    $<$<BOOL:${JACOBI_WARNINGS_AS_ERRORS}>:-Werror>)
  if(JACOBI_SANITIZERS)
    target_compile_options(jacobi_flags INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(jacobi_flags INTERFACE -fsanitize=address,undefined)
  endif()
endif()

# --- Biblioteca núcleo ------------------------------------------------------------
add_library(jacobi src/jacobi.cpp)
add_library(jacobi::jacobi ALIAS jacobi)
target_include_directories(jacobi PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_link_libraries(jacobi PRIVATE jacobi_flags)
target_compile_features(jacobi PUBLIC cxx_std_23)

# --- CLI ---------------------------------------------------------------------------
if(JACOBI_BUILD_CLI)
  add_executable(jacobi_cli src/main.cpp)
  target_link_libraries(jacobi_cli PRIVATE jacobi::jacobi jacobi_flags)
  install(TARGETS jacobi_cli RUNTIME DESTINATION bin)
endif()

# --- Tests -------------------------------------------------------------------------
if(JACOBI_BUILD_TESTS)
  enable_testing()
  find_package(Catch2 3 QUIET)
  if(NOT Catch2_FOUND)
    include(FetchContent)
    FetchContent_Declare(Catch2
      GIT_REPOSITORY https://github.com/catchorg/Catch2.git
      GIT_TAG v3.7.1
      GIT_SHALLOW TRUE)
    FetchContent_MakeAvailable(Catch2)
    list(APPEND CMAKE_MODULE_PATH ${catch2_SOURCE_DIR}/extras)
  endif()
  add_executable(test_jacobi tests/test_jacobi.cpp)
  target_link_libraries(test_jacobi PRIVATE jacobi::jacobi jacobi_flags Catch2::Catch2WithMain)
  include(Catch)
  catch_discover_tests(test_jacobi)
endif()

# --- Módulo de Python ----------------------------------------------------------------
if(JACOBI_BUILD_PYTHON)
  find_package(Python 3.11 REQUIRED COMPONENTS Interpreter Development.Module
               OPTIONAL_COMPONENTS Development.SABIModule)
  find_package(nanobind CONFIG REQUIRED)
  # STABLE_ABI: una sola wheel sirve para Python >= 3.12.
  nanobind_add_module(jacobi_nativo STABLE_ABI NB_STATIC python/bindings.cpp)
  target_link_libraries(jacobi_nativo PRIVATE jacobi::jacobi)
  install(TARGETS jacobi_nativo LIBRARY DESTINATION .)
endif()
