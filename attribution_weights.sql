{# macros/attribution_weights.sql #}

{#
  Returns the attribution weight for a single touchpoint row
  based on the selected model.

  Models:
    - first_touch:    100% to first touchpoint
    - last_touch:     100% to last touchpoint
    - linear:         Equal weight across all touchpoints (1/N each)
    - time_decay:     Exponential decay; more recent touchpoints get more credit.
                      Half-life = 7 days. Weight = 2^(-(days_before_conversion)/7)
                      Then normalized so weights sum to 1 per user.
    - u_shaped:       40% first, 40% last, remaining 20% split evenly among middle
#}

{% macro attribution_weight(model_type) %}
    case
        when '{{ model_type }}' = 'first_touch' then
            case when touchpoint_position = 1 then 1.0 else 0.0 end

        when '{{ model_type }}' = 'last_touch' then
            case when touchpoint_position = total_touchpoints then 1.0 else 0.0 end

        when '{{ model_type }}' = 'linear' then
            1.0 / total_touchpoints

        when '{{ model_type }}' = 'time_decay' then
            -- Raw weight: 2^(-(days_to_conversion / 7))
            -- Normalization happens in the mart layer via window function
            power(2, -1.0 * (
                datediff('day', event_timestamp, conversion_timestamp)
            ) / 7.0)

        when '{{ model_type }}' = 'u_shaped' then
            case
                when total_touchpoints = 1 then 1.0
                when touchpoint_position = 1 then 0.4
                when touchpoint_position = total_touchpoints then 0.4
                else 0.2 / greatest(total_touchpoints - 2, 1)
            end

        else null
    end
{% endmacro %}
