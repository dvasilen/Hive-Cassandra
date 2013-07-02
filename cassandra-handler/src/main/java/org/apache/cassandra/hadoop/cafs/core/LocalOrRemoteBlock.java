
package org.apache.cassandra.hadoop.cafs.core;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;

public class LocalOrRemoteBlock  
{

  public ByteBuffer remote_block;
  public LocalBlock local_block;

  public LocalOrRemoteBlock() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LocalOrRemoteBlock(LocalOrRemoteBlock other) {
    if (other.isSetRemote_block()) {
      this.remote_block = org.apache.thrift.TBaseHelper.copyBinary(other.remote_block);
;
    }
    if (other.isSetLocal_block()) {
      this.local_block = new LocalBlock(other.local_block);
    }
  }

  public LocalOrRemoteBlock deepCopy() {
    return new LocalOrRemoteBlock(this);
  }


  public void clear() {
    this.remote_block = null;
    this.local_block = null;
  }

  public byte[] getRemote_block() {
    setRemote_block(org.apache.thrift.TBaseHelper.rightSize(remote_block));
    return remote_block == null ? null : remote_block.array();
  }

  public ByteBuffer bufferForRemote_block() {
    return remote_block;
  }

  public LocalOrRemoteBlock setRemote_block(byte[] remote_block) {
    setRemote_block(remote_block == null ? (ByteBuffer)null : ByteBuffer.wrap(remote_block));
    return this;
  }

  public LocalOrRemoteBlock setRemote_block(ByteBuffer remote_block) {
    this.remote_block = remote_block;
    return this;
  }

  public void unsetRemote_block() {
    this.remote_block = null;
  }

  /** Returns true if field remote_block is set (has been assigned a value) and false otherwise */
  public boolean isSetRemote_block() {
    return this.remote_block != null;
  }

  public void setRemote_blockIsSet(boolean value) {
    if (!value) {
      this.remote_block = null;
    }
  }

  public LocalBlock getLocal_block() {
    return this.local_block;
  }

  public LocalOrRemoteBlock setLocal_block(LocalBlock local_block) {
    this.local_block = local_block;
    return this;
  }

  public void unsetLocal_block() {
    this.local_block = null;
  }

  /** Returns true if field local_block is set (has been assigned a value) and false otherwise */
  public boolean isSetLocal_block() {
    return this.local_block != null;
  }

  public void setLocal_blockIsSet(boolean value) {
    if (!value) {
      this.local_block = null;
    }
  }



  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LocalOrRemoteBlock)
      return this.equals((LocalOrRemoteBlock)that);
    return false;
  }

  public boolean equals(LocalOrRemoteBlock that) {
    if (that == null)
      return false;

    boolean this_present_remote_block = true && this.isSetRemote_block();
    boolean that_present_remote_block = true && that.isSetRemote_block();
    if (this_present_remote_block || that_present_remote_block) {
      if (!(this_present_remote_block && that_present_remote_block))
        return false;
      if (!this.remote_block.equals(that.remote_block))
        return false;
    }

    boolean this_present_local_block = true && this.isSetLocal_block();
    boolean that_present_local_block = true && that.isSetLocal_block();
    if (this_present_local_block || that_present_local_block) {
      if (!(this_present_local_block && that_present_local_block))
        return false;
      if (!this.local_block.equals(that.local_block))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_remote_block = true && (isSetRemote_block());
    builder.append(present_remote_block);
    if (present_remote_block)
      builder.append(remote_block);

    boolean present_local_block = true && (isSetLocal_block());
    builder.append(present_local_block);
    if (present_local_block)
      builder.append(local_block);

    return builder.toHashCode();
  }

  public int compareTo(LocalOrRemoteBlock other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    LocalOrRemoteBlock typedOther = (LocalOrRemoteBlock)other;

    lastComparison = Boolean.valueOf(isSetRemote_block()).compareTo(typedOther.isSetRemote_block());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRemote_block()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.remote_block, typedOther.remote_block);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLocal_block()).compareTo(typedOther.isSetLocal_block());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLocal_block()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.local_block, typedOther.local_block);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }



  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("LocalOrRemoteBlock(");
    boolean first = true;

    if (isSetRemote_block()) {
      sb.append("remote_block:");
      if (this.remote_block == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.remote_block, sb);
      }
      first = false;
    }
    if (isSetLocal_block()) {
      if (!first) sb.append(", ");
      sb.append("local_block:");
      if (this.local_block == null) {
        sb.append("null");
      } else {
        sb.append(this.local_block);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }



}

