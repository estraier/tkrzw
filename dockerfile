FROM gcc:13.1.0

WORKDIR /usr/local/workspace/tkrzw
ADD ./ /usr/local/workspace/tkrzw/

RUN ./configure --enable-opt-native --enable-most-features && make && make install
