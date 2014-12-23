#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
require 'java'

module Shell
  module Commands
    class Set 
      def help
        return <<-EOF
  Set a value to a key, for example:
  cmraft> set a 100
EOF
      end
      
      def usage
        return <<-EOF
  usage: set <key> <value>
  example: set mykey 12345
EOF
      end

      def command(*args)
        if(args.size != 2) then
          print usage
          return
        end
		#puts "set called" << args[0] << " :" << args[1]
		conn = Java::com.chicm.cmraft.ConnectionManager.getConnection()		
		kvs = conn.getKeyValueStore()
        kvs.set(args[0], args[1])
        conn.close()
      end
    end
  end
end

