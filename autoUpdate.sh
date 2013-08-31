#/bin/bash
username=""
password=""
git add src/*
git add pom.xml
git commit -m "rpc"
expect -c "set timeout -1;
    spawn git push origin master;
    expect {
      *sername:* {send -- $username\r;
        expect {
        	*assword* {send -- $password\r;
        		expect {
            		*denied* {exit 2;}
            		eof
        		}
      		}
            *denied* {exit 2;}
            eof
        }
      }
      
      eof  {exit 1;}
    }
    "
