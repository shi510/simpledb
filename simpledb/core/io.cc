#include "simpledb/core/io.h"

namespace simpledb{
namespace file{

file_io::~file_io(){
    close();
}

void file_io::open(std::string name,
                   std::ios::openmode mode
                  ){
	if (file.is_open()){
		throw std::string("File is already opened.");
	}
	file.open(name, mode);
    if(!file.is_open()){
        throw std::string("File can not open. - ") + name;
    }
}

void file_io::close(){
	if (file.is_open()){
		file.close();
	}
}

bool file_io::exist(std::string name){
    std::ifstream f(name);
    if(f.is_open()){
        f.close();
        return true;
    }
    else{
        return false;
    }
}

void file_io::create_file(std::string name){
    if(exist(name)){
        throw std::string("File exists. Can not create the file : ") + name;
    }
    std::ofstream f(name);
    if(f.good()){
        f.close();
    }
    else{
        throw std::string("File can not create. Error code : ") +
        std::to_string(f.rdstate());
    }
}

void file_io::read(char *ptr, uint64_t size){
    file.read(ptr, size);
}

uint64_t file_io::get_count_of_read(){
    return file.gcount();
}

void file_io::write(char *ptr, uint64_t size){
    file.write(ptr, size);
}

void file_io::seek_from_first_to(uint64_t pos){
    file.seekg(std::ios::beg + pos);
}

void file_io::seek_from_end_to(uint64_t pos){
    file.seekg(std::ios::end - pos);
}

void file_io::seek_to_first(){
    file.seekg(std::ios::beg);
}

void file_io::seek_to_end(){
    file.seekp(std::ios::beg, std::ios::end);
}

uint64_t file_io::get_position(){
    return file.tellg();
}

bool file_io::is_open(){
    return file.is_open();
}

void file_io::flush(){
    file.flush();
}

} // end namespace file
} // end namespace simpledb
