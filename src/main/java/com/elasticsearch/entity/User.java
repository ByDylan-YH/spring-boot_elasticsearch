package com.elasticsearch.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Author:BYDylan
 * Date:2020/5/13
 * Description:idea 需添加 lombok 插件才会生效
 * Data : 注解在类上,为类提供读写属性
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Component
public class User {
    private String name;
    private int age;
}
