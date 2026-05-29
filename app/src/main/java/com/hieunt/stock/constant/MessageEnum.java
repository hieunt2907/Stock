package com.hieunt.stock.constant;

public enum MessageEnum {

    ERROR_SYS_UNKNOW("error.sys.unknow", "-1"),
    ERROR_VALIDATE_NOTFOUND("error.validate.notFound", "102"),
    ERROR_USER_LOGIN_FAIL("error.user.login.fail", "106"),
    ERROR_USER_INACTIVE("error.user.inactive", "107"),
    ERROR_USER_NOTFOUND("error.user.notFound", "108"),
    IMAGE_EXTENSION_NOT_SUPPORT("Định dạng ảnh không được hỗ trợ", "109"),
    ERROR_EXISTED("error.existed", "201"),

    ERROR_CHA_DELETE("error.cha.delete", "202"),
    ERROR_CHA_UPDATE("error.cha.update", "203"),
    NOT_FOUND("not.found", "404"),
    ;

    private final String key;
    private final String code;

    public String getKey() {
        return this.key;
    }

    public String getCode() {
        return code;
    }

    MessageEnum(String key, String code) {
        this.key = key;
        this.code = code;
    }
}
