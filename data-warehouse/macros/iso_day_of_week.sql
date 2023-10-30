{%- macro iso_day_of_week(date) -%}

    case
        -- Shift start of week from Sunday (1) to Monday (2)
        when extract(dow from {{ date }}) = 0 then 7
        else extract(dow from {{ date }})
    end

{%- endmacro %}
