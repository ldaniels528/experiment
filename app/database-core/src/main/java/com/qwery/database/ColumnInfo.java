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

    boolean isRowID() default false;

    boolean isNullable() default true;

    boolean isCompressed() default false;

    int maxSize() default 0;

}
