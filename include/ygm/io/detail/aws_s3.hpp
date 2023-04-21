#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/Object.h>
#include <chrono>
#include <thread>

namespace ygm::io::detail {

class aws_options_init {
 public:
  aws_options_init() {
    //options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    InitAPI(options);
  }
  ~aws_options_init() { ShutdownAPI(options); }

 private:
  Aws::SDKOptions options;
};

class aws_line_reader {
 public:
  aws_line_reader(const std::string& bucket, const std::string& object,
                  size_t byte_offset = 0) {
    using namespace Aws;
    request.SetBucket(bucket);
    request.SetKey(object);
    if (byte_offset > 0) {
      std::stringstream sstr;
      sstr << "bytes=" << byte_offset << "-";
      request.SetRange(sstr.str());
    }

    size_t attempts = 0;
    do {
    outcome = client.GetObject(request);
     if(!outcome.IsSuccess()) {
       if(++attempts > 10) {
         break;
       }
       std::this_thread::sleep_for(5ms)
     }
    } while(!outcome.IsSuccess());

    if (!outcome.IsSuccess()) {
      const Aws::S3::S3Error& err = outcome.GetError();
      std::cerr << "Error: GetObject: " << err.GetExceptionName() << ": "
                << err.GetMessage() << std::endl;
    } else {
      if (byte_offset > 0) {
        // skip first line
        std::string line;
        if (std::getline(outcome.GetResult().GetBody(), line)) {
          m_bytes_read += line.size() + 1;
        }
      }
    }
  }

  bool getline(std::string& line) {
    line.clear();
    if (std::getline(outcome.GetResult().GetBody(), line)) {
      m_bytes_read += line.size() + 1;
      return true;
    }
    return false;
  }

  size_t bytes_read() const { return m_bytes_read; }

 private:
  aws_options_init                 aoi;
  Aws::S3::S3Client                client;
  Aws::S3::Model::GetObjectRequest request;
  Aws::S3::Model::GetObjectOutcome outcome;
  size_t                           m_bytes_read = 0;
};

std::vector<std::pair<std::string, size_t>> aws_list_objects(
    const std::string& bucket, const std::string& prefix) {
  using namespace Aws;
  SDKOptions options;
  options.loggingOptions.logLevel = Utils::Logging::LogLevel::Debug;
  std::vector<std::pair<std::string, size_t>> to_return;
  InitAPI(options);
  {
    S3::S3Client                       client;
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucket);
    request.SetPrefix(prefix);
    auto outcome = client.ListObjects(request);

    if (!outcome.IsSuccess()) {
      std::cerr << "Error: ListObjects: " << outcome.GetError().GetMessage()
                << std::endl;
    } else {
      Aws::Vector<Aws::S3::Model::Object> objects =
          outcome.GetResult().GetContents();

      for (Aws::S3::Model::Object& object : objects) {
        std::cout << object.GetKey() << " " << object.GetSize() << std::endl;
        if(object.GetSize() > 0) {
          to_return.push_back({object.GetKey(), object.GetSize()});
        }
      }
    }
  }
  ShutdownAPI(options);
  return to_return;
}

}  // namespace ygm::io::detail
