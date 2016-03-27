---
title: Content Event
layout: documentation
documentation: true
---
A message or an event is called Content Event in SAMOA. As the name suggests, it is an event which contains content which needs to be processed by the processors.

###1. Implementation
ContentEvent has been implemented as an interface in SAMOA. Users need to implement `ContentEvent` interface to create their custom message classes. As it can be seen in the following code, key is the necessary part of a message.

```
package org.apache.samoa.core;

public interface ContentEvent extends java.io.Serializable {
	
	public String getKey();
	
	public void setKey(String str);
	
	public boolean isLastEvent();
}
```
###2. Methods
Following is a brief description of methods.

#####2.1 `String getKey()`
Each message is identified by a key in SAMOA. All user-defined message classes should have a key state variable. Each instance of the custom message should be assigned a key. This method should return the key of the respective message.

#####2.2 `void setKey(String str)`
This method is used to assign a key to the message.

#####2.3 `boolean isLastEvent()`
This method lets SAMOA know that this message is the last message.

###3. Example
Following is the example of a `Message` class which implements `ContentEvent` interface. As `ContentEvent` is an interface, it can not hold variables. A user-defined message class should have its own data variables and its getter methods. In the following example, `value` variable of type `Object` is added to the class. Using a generic type `Object` is beneficial in the sense that any object can be passed to it and later it can be casted back to the original type. The following example also adds a `streamId` variable which stores the `id` of the stream the message belongs to. This is not a requirement but can be beneficial in certain applications.

```
import org.apache.samoa.core.ContentEvent;

/**
 * A general key-value message class which adds a stream id in the class variables
 * Stream id information helps in determining to which stream does the message belongs to.
 */
public class Message implements ContentEvent {

	/**
	 * To tell if the message is the last message of the stream. This may be required in some applications where
	 * a stream can cease to exist
	 */
	private boolean last=false;
	/**
	 * Id of the stream to which the message belongs
	 */
	private String streamId;
	/**
	 * The key of the message. Can be any sting value. Duplicates are allowed.
	 */
	private String key;
	/**
	 * The value of the message. Can be any object. Casting may be necessary to the desired type.
	 */
	private Object value;
	
	public Message()
	{}

	/**
	 * @param key
	 * @param value
	 * @param isLastEvent
	 * @param streamId
	 */
	public Message(String key, Object value, boolean isLastEvent, String streamId)
	{
		this.key=key;
		this.value = value;
		this.last = isLastEvent;
		this.streamId=streamId;
	}
	
	@Override
	public String getKey() {
		return key;
	}

	@Override
	public void setKey(String str) {
		this.key = str;
	}

	@Override
	public boolean isLastEvent() {
		return last;
	}
	
	/**
	 * @return value of the message
	 */
	public String getValue()
	{
		return value.toString();
	}
	
	/**
	 * @return id of the stream to which the message belongs
	 */
	public String getStreamId() {
		return streamId;
	}
	/**
	 * @param streamId
	 */
	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

}

```
