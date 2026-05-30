package com.hieunt.stock.controller;

import java.util.List;

import javax.validation.Valid;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import com.hieunt.stock.constant.SuccessMessageKey;
import com.hieunt.stock.exception.HIEUNTException;
import com.hieunt.stock.model.BaseResponse;
import com.hieunt.stock.service.BaseService;
import com.hieunt.stock.util.SecurityUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * BaseController sử dụng resource_access roles cho method security.
 * 
 * Mỗi controller con cần override getPermissionPrefix() để trả về prefix
 * tương ứng.
 * Ví dụ: prefix = "spec" → authority cần có: spec:create, spec:read,
 * spec:update, spec:delete
 * prefix = "user" → authority cần có: user:create, user:read,
 * user:update, user:delete
 */
@Slf4j
public abstract class BaseController<D> {
    protected abstract BaseService<D> getBaseService();

    /**
     * Trả về permission prefix cho controller.
     * Ví dụ: "spec", "user", "product", ...
     * Sẽ được dùng để check authority dạng: prefix:create, prefix:read, ...
     */
    protected abstract String getPermissionPrefix();

    protected List<String> getAllowedRoles() {
        return List.of();
    }

    protected void checkAccess(String action) {
        SecurityUtil.checkAnyRole(getAllowedRoles());
        SecurityUtil.checkPermission(getPermissionPrefix(), action);
    }

    @PostMapping("/create")
    public ResponseEntity<BaseResponse<D>> create(@Valid @RequestBody D dto) throws HIEUNTException {
        checkAccess("create");
        dto = getBaseService().create(dto);
        BaseResponse<D> baseResponse = new BaseResponse<>();
        baseResponse.setCode(HttpStatus.CREATED);
        baseResponse.setMessage(SuccessMessageKey.CREATE);
        baseResponse.setData(dto);
        return ResponseEntity.ok(baseResponse);
    }

    @PutMapping("/update")
    public ResponseEntity<BaseResponse<D>> update(@RequestParam("id") Long id, @Valid @RequestBody D dto)
            throws HIEUNTException {
        checkAccess("update");
        getBaseService().update(id, dto);
        BaseResponse<D> baseResponse = new BaseResponse<>();
        baseResponse.setCode(HttpStatus.OK);
        baseResponse.setMessage(SuccessMessageKey.UPDATE);
        baseResponse.setData(dto);
        return ResponseEntity.ok(baseResponse);
    }

    @DeleteMapping("/delete")
    public ResponseEntity<BaseResponse<D>> delete(@RequestParam("id") Long id) throws HIEUNTException {
        checkAccess("delete");
        getBaseService().delete(id);
        D dto = getBaseService().findById(id);
        BaseResponse<D> baseResponse = new BaseResponse<>();
        baseResponse.setCode(HttpStatus.OK);
        baseResponse.setMessage(SuccessMessageKey.DELETE);
        baseResponse.setData(dto);
        return ResponseEntity.ok(baseResponse);
    }

    @GetMapping("/findById")
    public ResponseEntity<BaseResponse<D>> findById(@RequestParam("id") Long id) throws HIEUNTException {
        checkAccess("read");
        D dto = getBaseService().findById(id);
        BaseResponse<D> baseResponse = new BaseResponse<>();
        baseResponse.setCode(HttpStatus.OK);
        baseResponse.setMessage(SuccessMessageKey.SUCCESS);
        baseResponse.setData(dto);
        return ResponseEntity.ok(baseResponse);
    }

    @GetMapping("/search")
    public ResponseEntity<BaseResponse<Page<D>>> search(
            @RequestParam(value = "filter", required = false) String filter,
            Pageable pageable) throws HIEUNTException {
        checkAccess("read");
        Page<D> dtos = getBaseService().search(filter, pageable);
        BaseResponse<Page<D>> baseResponse = new BaseResponse<>();
        baseResponse.setCode(HttpStatus.OK);
        baseResponse.setMessage(SuccessMessageKey.SUCCESS);
        baseResponse.setData(dtos);
        return ResponseEntity.ok(baseResponse);
    }

    @GetMapping("/findAll")
    public ResponseEntity<BaseResponse<List<D>>> findAll() throws HIEUNTException {
        checkAccess("read");
        List<D> dtos = getBaseService().findAll();
        BaseResponse<List<D>> baseResponse = new BaseResponse<>();
        baseResponse.setCode(HttpStatus.OK);
        baseResponse.setMessage(SuccessMessageKey.SUCCESS);
        baseResponse.setData(dtos);
        return ResponseEntity.ok(baseResponse);
    }
}
