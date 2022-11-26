from conans import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout

class GclConan(ConanFile):
    name = "gcl"
    version = "1.0.1"
    license = "MIT"
    url = "https://github.com/bloomen/gcl"
    description = "Conan package for bloomen/gcl."
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}
    exports_sources = "CMakeLists.txt", "include/*", "src/*"

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["gcl"]
