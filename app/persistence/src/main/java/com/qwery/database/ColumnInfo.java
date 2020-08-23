package com.qwery.database;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Column Information
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER, ElementType.CONSTRUCTOR, ElementType.FIELD})
public @interface ColumnInfo {

    boolean isNullable() default true;

    boolean isPrimary() default false;

    boolean isCompressed() default false;

    boolean isEncrypted() default false;

    int maxSize() default 0;

}
