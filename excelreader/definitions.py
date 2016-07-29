#!/usr/bin/env python
# -*- coding: utf-8 -*-

class Definitions:

    def __init__(self, _filepath):
        self.config={}
        f=open(_filepath)
        for line in f:
            line=line.replace("\n","")
            tok=line.split("=")
            if len(tok)!=2:
                continue
            else:
                self.config[tok[0]]=tok[1]

    def get(self,k):
        return self.config[k]


