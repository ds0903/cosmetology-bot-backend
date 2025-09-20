from typing import List, Dict
from datetime import datetime, timedelta

def recalculate_slots_for_duration(available_slots: List[str], time_fraction: int) -> List[str]:
    """
    Пересчитывает доступные слоты для услуг с длительностью > 30 минут.
    """
    if time_fraction == 1:
        return available_slots
    
    valid_slots = []
    slot_times = []
    
    for slot in available_slots:
        try:
            hour, minute = map(int, slot.split(':'))
            slot_time = datetime(2000, 1, 1, hour, minute)
            slot_times.append((slot, slot_time))
        except:
            continue
    
    for slot_str, slot_time in slot_times:
        can_fit = True
        
        for i in range(1, time_fraction):
            next_time = slot_time + timedelta(minutes=30 * i)
            next_slot_str = f"{next_time.hour:02d}:{next_time.minute:02d}"
            
            if next_slot_str not in available_slots:
                can_fit = False
                break
        
        if can_fit:
            valid_slots.append(slot_str)
    
    return valid_slots

def apply_duration_to_all_specialists(slots_dict: Dict, time_fraction: int) -> Dict:
    """
    Применяет пересчет available_slots для всех специалистов.
    """
    if time_fraction == 1:
        return slots_dict
    
    result = {}
    for specialist, slots in slots_dict.items():
        if isinstance(slots, list):
            result[specialist] = recalculate_slots_for_duration(slots, time_fraction)
        else:
            result[specialist] = slots
    
    return result

def recalculate_reserved_slots_for_duration(reserved_slots: List[str], time_fraction: int, all_work_slots: List[str]) -> List[str]:
    """
    Пересчитывает зарезервированные слоты для услуг с длительностью > 30 минут.
    Добавляет только слоты НАЗАД от занятых.
    """
    if time_fraction == 1:
        return reserved_slots
    
    expanded_reserved = set(reserved_slots)
    # TEMP DEBUG
    import sys
    print(f"DEBUG RESERVED: input={reserved_slots}, time_fraction={time_fraction}, work_slots_count={len(all_work_slots)}", file=sys.stderr)
    
    for slot in reserved_slots:
        try:
            hour, minute = map(int, slot.split(':'))
            slot_time = datetime(2000, 1, 1, hour, minute)
            
            # Добавляем только предыдущие слоты
            for i in range(1, time_fraction):
                prev_time = slot_time - timedelta(minutes=30 * i)
                prev_slot = f"{prev_time.hour:02d}:{prev_time.minute:02d}"
                print(f"DEBUG: Checking {prev_slot} in work_slots: {prev_slot in all_work_slots}", file=sys.stderr)
                if prev_slot in all_work_slots:
                    expanded_reserved.add(prev_slot)
                    print(f"DEBUG: Added {prev_slot}, expanded now: {expanded_reserved}", file=sys.stderr)
        except:
            continue
    
    return sorted(list(expanded_reserved))

def apply_reserved_duration_to_all_specialists(reserved_dict: Dict, available_dict: Dict, time_fraction: int) -> Dict:
    """
    Применяет пересчет reserved_slots для всех специалистов.
    """
    if time_fraction == 1:
        return reserved_dict
    
    result = {}
    for specialist, reserved_slots in reserved_dict.items():
        if isinstance(reserved_slots, list):
            all_work_slots = []
            specialist_name = specialist.replace('reserved_slots_', '')
            available_key = f'available_slots_{specialist_name}'
            if available_key in available_dict and isinstance(available_dict[available_key], list):
                all_work_slots = available_dict[available_key] + reserved_slots
            result[specialist] = recalculate_reserved_slots_for_duration(
                reserved_slots, 
                time_fraction, 
                sorted(list(set(all_work_slots)))
            )
        else:
            result[specialist] = reserved_slots
    
    return result
