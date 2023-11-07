#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/file.hh>
#include <iostream>

// seastar::future<> f() {
//     std::cout << "Sleeping... " << std::flush;
//     using namespace std::chrono_literals;
// 
//     return seastar::sleep(1s).then([] {
//         std::cout << "Done.\n";
//     });
// 
//     // auto &&fut1 = seastar::sleep(1s);
//     // auto &&fut2 = fut1.then([] {
//     //     std::cout << "Done.\n";
//     // });
//     // return std::move(fut2);
// }

seastar::future<> f() {
    seastar::thread th([] {
        std::cout << "Hi.\n";
        for (int i = 1; i < 4; i++) {
            seastar::sleep(std::chrono::seconds(1)).get();
            std::cout << i << "\n";
        }
    });
    return do_with(std::move(th), [] (auto& th) {
        return th.join();
    });
}

