#ifndef __FILE_IO_H__
#define __FILE_IO_H__
#include <string>
#include <fstream>

namespace simpledb{
namespace file{

class file_io{
public:
    virtual ~file_io();

    void open(std::string name,
              std::ios::openmode mode
             );

	void close();

    bool exist(std::string name);

    void create_file(std::string name);

    void read(char *ptr, uint64_t size);

    uint64_t get_count_of_read();

    void write(char *ptr, uint64_t size);

    void seek_from_first_to(uint64_t pos);

    void seek_from_end_to(uint64_t pos);

    void seek_to_first();

    void seek_to_end();

    uint64_t get_position();

    bool is_open();

    void flush();

private:
	std::fstream file;
};

} // end namespace file
} // end namespace simpledb
#endif //end #ifndef __FILE_IO_H__
