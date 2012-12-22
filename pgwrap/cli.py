
import code,sys
import pgwrap
        
if __name__ == '__main__':
    db = pgwrap.db.connection()
    code.interact(local=locals())
