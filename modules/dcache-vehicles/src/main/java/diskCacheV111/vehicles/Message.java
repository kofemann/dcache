// $Id: Message.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import javax.security.auth.Subject;

import java.io.Serializable;

import dmg.cells.nucleus.HasDiagnosticContext;

import org.dcache.auth.Subjects;

// Base class for all Messages

public class Message
    implements Serializable,
               HasDiagnosticContext
{
    private boolean _replyRequired;
    private boolean _isReply;
    private int     _returnCode;
    private Object  _errorObject;
    private long    _id;
    private Subject _subject;

    private static final long serialVersionUID = 2056896713066252504L;

    public Message(){
    }

    public Message(boolean replyRequired){
	_replyRequired = replyRequired;
    }
    @Override
    public String toString(){
        return _returnCode==0?"":"("+_returnCode+")="+_errorObject ;
    }
    public void setSucceeded(){
	setReply(0,null);
    }

    public void setFailedConditionally(int rc, Serializable errorObject)
    {
        if (getReturnCode() == 0) {
            setFailed(rc, errorObject);
        }
    }

    public void setFailed(int errorCode, Serializable errorObject){
	setReply(errorCode, errorObject);
    }
    public void setReply(){
        _isReply = true ;
    }
    public void setReply(int returnCode, Serializable errorObject){
	_isReply     = true;
	_returnCode  = returnCode;
	_errorObject = errorObject;
    }

    public boolean isReply(){
	return _isReply;
    }

    public void clearReply(){
	//allows us to reuse message objects
	_isReply     = false;
	_returnCode  = 0;
	_errorObject = null;
    }

    public int getReturnCode(){
	return _returnCode;
    }

    public Serializable getErrorObject(){
	return (Serializable) _errorObject;
    }

    public boolean getReplyRequired(){
	return _replyRequired;
    }

    public void setReplyRequired(boolean replyRequired){
	_replyRequired = replyRequired;
    }
    public void setId( long id ){ _id = id ; }
    public long getId(){ return _id ; }

    public void setSubject(Subject subject)
    {
        _subject = subject;
    }

    public Subject getSubject()
    {
        return (_subject == null) ? Subjects.ROOT : _subject;
    }

    /**
     * Returns a human readable name of the message class. By default
     * this is the short class name with the "Message" or "Msg" suffix
     * removed.
     */
    public String getMessageName()
    {
        String name = getClass().getSimpleName();
        int length = name.length();
        if ((length > 7) && name.endsWith("Message")) {
            name = name.substring(0, name.length() - 7);
        } else if ((length > 3) && name.endsWith("Msg")) {
            name = name.substring(0, name.length() - 3);
        }

        return name;
    }

    @Override
    public String getDiagnosticContext()
    {
        return getMessageName();
    }

}

