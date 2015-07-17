#!/usr/bin/env python

import tornado.ioloop
import tornado.web

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Test")


app = tornado.web.Application([(r"/", MainHandler)])

if __name__ == "__main__":
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()
