// Force KUZU_C_API to resolve to __declspec(dllimport) on Windows so SWIG
// sees the functions, or to __attribute__((visibility("default"))) on Unix.
#ifdef _WIN32
#define KUZU_C_API __declspec(dllimport)
#else
#define KUZU_C_API __attribute__((visibility("default")))
#endif

%module kuzunet
%{
/* Put header files here or function declarations like below */
#include "kuzu.h"
%}

%include "typemaps.i"
%include "arrays_csharp.i"

// Map fixed-width integer types to C# equivalents
%apply unsigned char { uint8_t };
%apply signed char { int8_t };

%apply unsigned short { uint16_t };
%apply short { int16_t };

%apply unsigned int { uint32_t };
%apply int { int32_t };

%apply unsigned long long { uint64_t };
%apply long long { int64_t };

// Mark functions that allocate a fresh kuzu_value so SWIG sets swigCMemOwn=true
%newobject kuzu_value_create_null;
%newobject kuzu_value_create_null_with_data_type;
%newobject kuzu_value_create_default;
%newobject kuzu_value_create_bool;
%newobject kuzu_value_create_int8;
%newobject kuzu_value_create_int16;
%newobject kuzu_value_create_int32;
%newobject kuzu_value_create_int64;
%newobject kuzu_value_create_uint8;
%newobject kuzu_value_create_uint16;
%newobject kuzu_value_create_uint32;
%newobject kuzu_value_create_uint64;
%newobject kuzu_value_create_int128;
%newobject kuzu_value_create_float;
%newobject kuzu_value_create_double;
%newobject kuzu_value_create_internal_id;
%newobject kuzu_value_create_date;
%newobject kuzu_value_create_timestamp_ns;
%newobject kuzu_value_create_timestamp_ms;
%newobject kuzu_value_create_timestamp_sec;
%newobject kuzu_value_create_timestamp_tz;
%newobject kuzu_value_create_timestamp;
%newobject kuzu_value_create_interval;
%newobject kuzu_value_create_string;
%newobject kuzu_value_clone;


// --------------
// Pass strings around letting C# handle marshalling
%typemap(cstype) (char **out_result) "out string";
%typemap(imtype) (char **out_result) "out string";
%typemap(csin) (char **out_result) "out $csinput";
%typemap(argout) (char **out_result) {
  if (*$1) {
    char *tmp = *$1;
    *$1 = SWIG_csharp_string_callback((const char *)tmp);
    kuzu_destroy_string(tmp);
  }
}

// Hide kuzu_destroy_string/blob from the public C# API.
// SWIG's string callback copies char* returns to managed memory but does NOT free
// the native pointer. The %typemap(ret) below calls kuzu_destroy_string in the C
// wrapper after the callback has copied the data, preventing native string leaks.
%ignore kuzu_destroy_string;
%ignore kuzu_destroy_blob;

// Free native strings returned by C API functions. kuzu_destroy_string is still
// available in the C wrapper because kuzu.h is included in the %{ %} block above.
%typemap(ret) char* kuzu_prepared_statement_get_error_message,
              char* kuzu_query_result_get_error_message,
              char* kuzu_query_result_to_string,
              char* kuzu_flat_tuple_to_string,
              char* kuzu_value_to_string,
              char* kuzu_get_version
%{
  if ($1) kuzu_destroy_string($1);
%}
%typemap(cstype) (uint8_t **out_result) "out byte[]";
%typemap(imtype) (uint8_t **out_result) "out byte[]";
%typemap(csin) (uint8_t **out_result) "out $csinput";

// Correct borrowed kuzu_value** patterns -> out kuzu_value (non-owning wrapper)
// Remove previous incorrect direct mapping.
%typemap(cstype) (kuzu_value **out_value) "out kuzu_value";
%typemap(imtype) (kuzu_value **out_value) "out kuzu_value";
%typemap(csin) (kuzu_value **out_value) "out $csinput";

%typemap(cstype) (kuzu_value **out_key) "out kuzu_value";
%typemap(imtype) (kuzu_value **out_key) "out kuzu_value";
%typemap(csin) (kuzu_value **out_key) "out $csinput";

// If needed additional out pointers can be added similarly.

%typemap(cstype) char **out_column_name "out string";
%typemap(imtype) char **out_column_name "out string";
%typemap(csin) char **out_column_name "out $csinput";
%typemap(argout) char **out_column_name {
  if (*$1) {
    char *tmp = *$1;
    *$1 = SWIG_csharp_string_callback((const char *)tmp);
    kuzu_destroy_string(tmp);
  }
}

%typemap(cstype) 
SWIGTYPE **elements, 
SWIGTYPE **values, 
SWIGTYPE **field_values,
SWIGTYPE **keys
"$1_basetype[]";

%typemap(imtype) 
SWIGTYPE **elements, 
SWIGTYPE **values, 
SWIGTYPE **field_values,
SWIGTYPE **keys
"$1_basetype[]"

%typemap(csin) 
SWIGTYPE **elements, 
SWIGTYPE **values, 
SWIGTYPE **field_values,
SWIGTYPE **keys
"$csinput"

// Arrow C Data Interface -- not yet supported.
// TODO: Proper C# interop for ArrowArray/ArrowSchema would require custom typemaps
//       or a dedicated managed wrapper (e.g. Apache.Arrow). Ignored for now.
%ignore ArrowArray;
%ignore ArrowSchema;
%ignore ARROW_FLAG_DICTIONARY_ORDERED;
%ignore ARROW_FLAG_MAP_KEYS_SORTED;
%ignore ARROW_FLAG_NULLABLE;
%ignore kuzu_query_result_get_arrow_schema;
%ignore kuzu_query_result_get_next_arrow_chunk;

// TODO: const char** field_names in kuzu_value_create_struct generates an unusable
//       SWIGTYPE_p_p_char proxy. A proper typemap mapping const char** → string[]
//       with MarshalAs attributes is needed to make struct creation callable from C#.


// --------------------
// Map the kuzu_value_get_{type} methods to use "out {type}"
%apply signed char *OUTPUT { int8_t *out_result };
%apply unsigned char *OUTPUT { uint8_t *out_result };

%apply short *OUTPUT { int16_t *out_result };
%apply unsigned short *OUTPUT { uint16_t *out_result };

%apply int *OUTPUT { int32_t *out_result};
%apply unsigned int *OUTPUT { uint32_t *out_result};

%apply long long *OUTPUT { int64_t *out_result};
%apply unsigned long long *OUTPUT { uint64_t *out_result};
%apply unsigned long long *OUTPUT { uint64_t *out_value};

%apply bool *OUTPUT { bool *out_result };
%apply float *OUTPUT { float *out_result };
%apply double *OUTPUT { double *out_result };


// ------ 
// Ignore private members of structs
%ignore _connection;
%ignore _database;
%ignore _flat_tuple;
%ignore _data_type;
%ignore _value;
%ignore _prepared_statement;
%ignore _query_result;
%ignore _query_summary;
%ignore _bound_values;
// Stop ignoring _is_owned_by_cpp so we could (optionally) inspect ownership.
//%ignore _is_owned_by_cpp;

// kuzu_logical_type needs a dedicated dispose because the C API destroy function
// is kuzu_data_type_destroy, not kuzu_logical_type_destroy (naming mismatch 
// between the struct typedef and the API convention).
%typemap(csdisposing, methodname="Dispose", methodmodifiers="protected", parameters="bool disposing") 
kuzu_logical_type
%{
  {
    lock(this) {
      if (swigCPtr.Handle != global::System.IntPtr.Zero) {
        if (swigCMemOwn) {
          swigCMemOwn = false;
          $modulePINVOKE.kuzu_data_type_destroy(kuzu_logical_type.getCPtr(this));
        }
        swigCPtr = new global::System.Runtime.InteropServices.HandleRef(null, global::System.IntPtr.Zero);
      }
    }
  }
%}

// Replace destructor and dispose to call destroy only when owning.
// This avoids double free of borrowed objects (e.g. kuzu_value returned by
// kuzu_flat_tuple_get_value is borrowed and must NOT be destroyed by C#).
%typemap(csdisposing, methodname="Dispose", methodmodifiers="protected", parameters="bool disposing") 
kuzu_connection, 
kuzu_database, 
kuzu_prepared_statement, 
kuzu_flat_tuple, 
kuzu_query_summary,
kuzu_query_result
%{
  {
    lock(this) {
      if (swigCPtr.Handle != global::System.IntPtr.Zero) {
        if (swigCMemOwn) {
          swigCMemOwn = false;
          $modulePINVOKE.$csclassname_destroy($csclassname.getCPtr(this));
        }
        swigCPtr = new global::System.Runtime.InteropServices.HandleRef(null, global::System.IntPtr.Zero);
      }
    }
  }
%}

// kuzu_value created via default constructor has no internal value pointer yet.
// In that case, call delete_kuzu_value instead of kuzu_value_destroy to avoid
// crashing in native code. For real values, keep using kuzu_value_destroy.
%inline %{
static int kuzu_value_has_internal(kuzu_value* value) {
    return (value && value->_value) ? 1 : 0;
}
%}

%typemap(csdisposing, methodname="Dispose", methodmodifiers="protected", parameters="bool disposing") 
kuzu_value
%{
  {
    lock(this) {
      if (swigCPtr.Handle != global::System.IntPtr.Zero) {
        if (swigCMemOwn) {
          swigCMemOwn = false;
          if ($modulePINVOKE.kuzu_value_has_internal(kuzu_value.getCPtr(this)) != 0) {
            $modulePINVOKE.kuzu_value_destroy(kuzu_value.getCPtr(this));
          } else {
            $modulePINVOKE.delete_kuzu_value(kuzu_value.getCPtr(this));
          }
        }
        swigCPtr = new global::System.Runtime.InteropServices.HandleRef(null, global::System.IntPtr.Zero);
      }
    }
  }
%}


// On Windows, include windows.i for SWIG to understand Win32 types.
// On Unix, this is not needed but harmless.
#ifdef _WIN32
%include <windows.i>
#endif
%include "kuzu.h"

// Workaround: SWIG doesn't automatically wrap <time.h>'s struct tm as a C# class,
// so we define it inline under #ifdef SWIG to generate a proper managed wrapper.
%inline %{
#ifdef SWIG
typedef struct {
    int tm_sec;   // seconds after the minute - [0, 60] including leap second
    int tm_min;   // minutes after the hour - [0, 59]
    int tm_hour;  // hours since midnight - [0, 23]
    int tm_mday;  // day of the month - [1, 31]
    int tm_mon;   // months since January - [0, 11]
    int tm_year;  // years since 1900
    int tm_wday;  // days since Sunday - [0, 6]
    int tm_yday;  // days since January 1 - [0, 365]
    int tm_isdst; // daylight savings time flag
} tm;
#endif
%}