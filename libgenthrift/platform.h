
#ifndef _LIBCASSANDRA_PLATFORM_HPP_
#define _LIBCASSANDRA_PLATFORM_HPP_


#define LIBCASSANDRA_PLATFORM_WINDOWS 0
#define LIBCASSANDRA_PLATFORM_LINUX   1
#define LIBCASSANDRA_PLATFORM_MAC     2


#if defined(__WIN32__) || defined(_WIN32)
// disable type needs to have dll-interface to be used byu clients due to STL member variables which are not public
#pragma warning (disable: 4251)
//disable warning about no suitable definition provided for explicit template instantiation request which seems to have no resolution nor cause any problems
#pragma warning (disable: 4661)
//disable non dll-interface class used as base for dll-interface class when deriving from singleton
#pragma warning (disable : 4275)
#  define LIBCASSANDRA_PLATFORM LIBCASSANDRA_PLATFORM_WINDOWS
#elif defined(__APPLE_CC__) || defined(__APPLE__)
#  define LIBCASSANDRA_PLATFORM LIBCASSANDRA_PLATFORM_MAC
#  ifndef __MACOSX__
#    define __MACOSX__
#  endif
#else
#  define LIBCASSANDRA_PLATFORM LIBCASSANDRA_PLATFORM_LINUX
#endif

#ifdef LIBCASSANDRA_DEBUG_BUILD
#define LIBCASSANDRA_DEBUG 1
#else
#define LIBCASSANDRA_DEBUG 0
#endif

#ifndef LIBCASSANDRA_EXPORT
# if LIBCASSANDRA_PLATFORM == LIBCASSANDRA_PLATFORM_WINDOWS
#   if defined(STATIC_LINKED)
#     define LIBCASSANDRA_EXPORT
#   else
#     if defined(LIBCASSANDRA_BUILD)
#       define LIBCASSANDRA_EXPORT __declspec(dllexport)
#       define LIBCASSANDRA_EXPORT_TEMPLATE
#     else
#       define LIBCASSANDRA_EXPORT __declspec(dllimport)
#       define LIBCASSANDRA_EXPORT_TEMPLATE extern
#     endif
#   endif
#   define LIBCASSANDRA_PLUGIN_EXPORT __declspec(dllexport)
# else
#   if defined(__GNUC__) && __GNUC__ >= 4
#     define LIBCASSANDRA_EXPORT __attribute__ ((visibility("default")))
#     define LIBCASSANDRA_PLUGIN_EXPORT __attribute__ ((visibility("default")))
#   else
#     define LIBCASSANDRA_EXPORT
#     define LIBCASSANDRA_PLUGIN_EXPORT
#   endif
#   define LIBCASSANDRA_EXPORT_TEMPLATE
# endif
#endif


#ifndef LIBCASSANDRA_FUNCTION_EXPORT
# if LIBCASSANDRA_PLATFORM == LIBCASSANDRA_PLATFORM_WINDOWS
#   if defined(STATIC_LINKED)
#     define LIBCASSANDRA_FUNCTION_EXPORT
#   else
#     if defined(LIBCASSANDRA_BUILD)
#       define LIBCASSANDRA_FUNCTION_EXPORT __declspec(dllexport)
#     else
#       define LIBCASSANDRA_FUNCTION_EXPORT __declspec(dllimport)
#     endif
#   endif
# else
#   if defined(__GNUC__) && __GNUC__ >= 4
#     define LIBCASSANDRA_FUNCTION_EXPORT __attribute__ ((visibility("default")))
#   else
#     define LIBCASSANDRA_FUNCTION_EXPORT
#   endif
# endif
#endif



#ifndef LIBCASSANDRA_EXPORT_C
# define LIBCASSANDRA_EXPORT_C extern "C" LIBCASSANDRA_EXPORT
#endif

#endif //_LIBCASSANDRA_PLATFORM_HPP_