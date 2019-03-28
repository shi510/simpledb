#ifndef __UTIL_H__
#define __UTIL_H__
#include <string>

namespace simpledb{
namespace util{

inline void runtime_assert(bool condition, std::string message){
    !condition ? throw message : 0;
}

} // end namespace util
} // end namespace simpledb
#endif // end #ifndef __UTIL_H__
